package org.apache.storm;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by anirudhnair on 3/8/16.
 */
public class RollingSort {

    public static final String SPOUT_ID = "spout";
    public static final String SORT_BOLT_ID ="sort";

    public StormTopology getTopology(Config config, ConfigReader reader) {
        final int spoutNum = reader.GetInstance("RollingSort", SPOUT_ID);
        final int boltNum =  reader.GetInstance("RollingSort", SORT_BOLT_ID);;
        final int msgSize = 100;
        final int chunkSize = 100;
        final int emitFreq = 100;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new RandomMessageSpout(), spoutNum);
        builder.setBolt(SORT_BOLT_ID, new SortBolt(emitFreq, chunkSize), boltNum).shuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }

    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(RollingSort.class);
    public class RandomMessageSpout extends BaseRichSpout {

        private static final long serialVersionUID = -4100642374496292646L;
        public static final String FIELDS = "message";
        public static final String MESSAGE_SIZE = "message.size";
        public static final int DEFAULT_MESSAGE_SIZE = 100;

        private final int sizeInBytes = DEFAULT_MESSAGE_SIZE;
        private long messageCount = 0;
        private SpoutOutputCollector collector;
        private String [] messages = null;
        private final boolean ackEnabled = true;
        private Random rand = null;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.rand = new Random();
            this.collector = collector;
            final int differentMessages = 100;
            this.messages = new String[differentMessages];
            for(int i = 0; i < differentMessages; i++) {
                StringBuilder sb = new StringBuilder(sizeInBytes);
                for(int j = 0; j < sizeInBytes; j++) {
                    sb.append(rand.nextInt(9));
                }
                messages[i] = sb.toString();
            }
        }

        @Override
        public void nextTuple() {
            final String message = messages[rand.nextInt(messages.length)];
            if(ackEnabled) {
                collector.emit(new Values(message), messageCount);
                messageCount++;
            } else {
                collector.emit(new Values(message));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class SortBolt extends BaseBasicBolt {

        public static final String EMIT_FREQ = "emit.frequency";
        public static final int DEFAULT_EMIT_FREQ = 60;  // 60s
        public static final String CHUNK_SIZE = "chunk.size";
        public static final int DEFAULT_CHUNK_SIZE = 100;
        public static final String FIELDS = "sorted_data";

        private final int emitFrequencyInSeconds;
        private final int chunkSize;
        private int index = 0;
        private MutableComparable[] data;


        public SortBolt(int emitFrequencyInSeconds, int chunkSize) {
            this.emitFrequencyInSeconds = emitFrequencyInSeconds;
            this.chunkSize = chunkSize;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            this.data = new MutableComparable[this.chunkSize];
            for (int i = 0; i < this.chunkSize; i++) {
                this.data[i] = new MutableComparable();
            }
        }

        public boolean isTickTuple(Tuple tuple) {
            return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
                    Constants.SYSTEM_TICK_STREAM_ID);
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            if (isTickTuple(tuple)) {
                Arrays.sort(data);
                basicOutputCollector.emit(new Values(data));
                LOG.info("index = " + index);
            } else {
                Object obj = tuple.getValue(0);
                if (obj instanceof Comparable) {
                    data[index].set((Comparable) obj);
                } else {
                    throw new RuntimeException("tuple value is not a Comparable");
                }
                index = (index + 1 == chunkSize) ? 0 : index + 1;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(FIELDS));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Map<String, Object> conf = new HashMap<String, Object>();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
            return conf;
        }
    }

    private static class MutableComparable implements Comparable, Serializable {
        private static final long serialVersionUID = -5417151427431486637L;
        private Comparable c = null;

        public MutableComparable() {

        }

        public MutableComparable(Comparable c) {
            this.c = c;
        }

        public void set(Comparable c) {
            this.c = c;
        }

        public Comparable get() {
            return c;
        }

        @Override
        public int compareTo(Object other) {
            if (other == null) return 1;
            Comparable oc = ((MutableComparable) other).get();
            if (null == c && null == oc) {
                return 0;
            } else if (null == c) {
                return -1;
            } else if (null == oc) {
                return 1;
            } else {
                return c.compareTo(oc);
            }
        }
    }




}
