package org.apache.storm;

import backtype.storm.Config;
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

import java.util.Map;
import java.util.Random;

/**
 * Created by anirudhnair on 3/8/16.
 */
public class SOL {
    public static final String BOLT_ID = "bolt";
    public static final String SPOUT_ID = "spout";



    public StormTopology getTopology(Config config, ConfigReader reader) {
        final int numLevels = reader.GetLevels("SOL",BOLT_ID);
        final int spoutNum = reader.GetInstance("SOL",SPOUT_ID);
        final int boltNum = reader.GetInstance("SOL",BOLT_ID);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, new RandomMessageSpout(), spoutNum);
        builder.setBolt(BOLT_ID + 1, new ConstBolt(), boltNum)
                .shuffleGrouping(SPOUT_ID);
        for (int levelNum = 2; levelNum <= numLevels - 1; levelNum++) {
            builder.setBolt(BOLT_ID + levelNum, new ConstBolt(), boltNum)
                    .shuffleGrouping(BOLT_ID + (levelNum - 1));
        }
        return builder.createTopology();
    }

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

    public class ConstBolt extends BaseBasicBolt {
        private static final long serialVersionUID = -5313598399155365865L;
        public static final String FIELDS = "message";

        public ConstBolt() {
        }

        @Override
        public void prepare(Map conf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            collector.emit(new Values(tuple.getValue(0)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

}
