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
import org.apache.storm.metrics.hdrhistogram.HistogramMetric;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by anirudhnair on 3/8/16.
 */
public class RollingSort implements ILBTopoIface{

    private static class Cookie {
        public final long time;

        Cookie(long time) {
            this.time = time;
        }
    }

    public static final String SPOUT_ID = "spout";
    public static final String SORT_BOLT_ID ="sort";

    public StormTopology getTopology(Config config) throws IOException {

        TopoConfigReader   oTopoConfig = new TopoConfigReader("/home/ajayaku2/conf/topo_config.xml");
        oTopoConfig.Init();
        LBConfigReader     oLBConfig = new LBConfigReader("/home/ajayaku2/conf/lb_config.xml");
        oLBConfig.Init();
        final int spoutNum = oTopoConfig.GetInstance("RollingSort", SPOUT_ID);
        final int boltNum =  oTopoConfig.GetInstance("RollingSort", SORT_BOLT_ID);
        final int msgSize = oTopoConfig.GetInstance("RollingSort", "msg_size");
        final int chunkSize = 100;
        final int emitFreq = oTopoConfig.GetInstance("RollingSort", "emit_freq");
        final long statCollectionInterval = Long.parseLong(oLBConfig.GetValue("STAT_COLLECTION","interval"));
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new RandomMessageSpout(msgSize,statCollectionInterval), spoutNum);
        builder.setBolt(SORT_BOLT_ID, new SortBolt(emitFreq, chunkSize), boltNum).shuffleGrouping(SPOUT_ID);

        config.setNumWorkers(oTopoConfig.GetInstance("RollingSort", "workers"));

        return builder.createTopology();
    }

    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(RollingSort.class);
    public class RandomMessageSpout extends BaseRichSpout {

        private static final long serialVersionUID = -4100642374496292646L;
        public static final String FIELDS = "message";
        public static final int DEFAULT_MESSAGE_SIZE = 100;
        private int sizeInBytes = DEFAULT_MESSAGE_SIZE;
        private long messageCount = 0;
        private SpoutOutputCollector collector;
        private String [] messages = null;
        private final boolean ackEnabled = true;
        private Random rand = null;
        private long stat_interval;
        private long start_time;
        private int rate_index = 1;
        private int [] data_rates;

        // variable to control data rate
        long _periodNano;
        long _emitAmount;
        Random _rand;
        long _nextEmitTime;
        long _emitsLeft;

        HistogramMetric _histo;

        public RandomMessageSpout(int msg_size, long statIntervalms) throws IOException {
            stat_interval = statIntervalms;
            sizeInBytes = msg_size;
            BufferedReader in = new BufferedReader(new java.io.FileReader("/home/ajayaku2/conf/data_rates.txt"));
            String str;
            data_rates = new int[720];
            int count = 0;
            while((str = in.readLine()) != null){
                data_rates[count] = Integer.parseInt(str);
                count++;
            }

            int ratePerSecond = data_rates[0];
            if (ratePerSecond > 0) {
                _periodNano = Math.max(1, 1000000000/ratePerSecond);
                _emitAmount = Math.max(1, (long)((ratePerSecond / 1000000000.0) * _periodNano));
            } else {
                _periodNano = Long.MAX_VALUE - 1;
                _emitAmount = 1;
            }

        }

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.rand = new Random();
            this.collector = collector;
            start_time = System.nanoTime();
            _nextEmitTime = System.nanoTime();
            _emitsLeft = _emitAmount;
            final int differentMessages = 100;
            this.messages = new String[differentMessages];
            for(int i = 0; i < differentMessages; i++) {
                StringBuilder sb = new StringBuilder(sizeInBytes);
                for(int j = 0; j < sizeInBytes; j++) {
                    sb.append(rand.nextInt(9));
                }
                messages[i] = sb.toString();
            }
            _histo = new HistogramMetric(3600000000000L, 3);
            context.registerMetric("comp-lat-histo", _histo, (int) stat_interval/1000);

            // read the data rates

        }

        @Override
        public void nextTuple() {

            if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
                _emitsLeft = _emitAmount;
                _nextEmitTime = _nextEmitTime + _periodNano;
            }

            if (_emitsLeft > 0) {
                String message = messages[rand.nextInt(messages.length)];
                collector.emit(new Values(message), new Cookie(System.nanoTime()));
                _emitsLeft--;
            }

            if(((System.nanoTime() - start_time) / 1000000) > (rate_index * Common.DATA_RATE_CHANGE_INTERVAL ))
            {
                int ratePerSecond = data_rates[rate_index];
                if (ratePerSecond > 0) {
                    _periodNano = Math.max(1, 1000000000/ratePerSecond);
                    _emitAmount = Math.max(1, (long)((ratePerSecond / 1000000000.0) * _periodNano));
                }
                rate_index++;
            }

        }

        @Override
        public void ack(Object id) {
            long end = System.nanoTime();
            Cookie st = (Cookie)id;
            _histo.recordValue(end-st.time);
        }

        @Override
        public void fail(Object id) {
            Cookie st = (Cookie)id;
            // not replaying messages
            //_collector.emit(new Values(st.sentence), id);
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class SortBolt extends BaseBasicBolt {

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
