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

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by anirudhnair on 3/8/16.
 */
public class WordCount {

    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String SPLIT_ID = "split";
    public static final String SPLIT_NUM = "component.split_bolt_num";
    public static final String COUNT_ID = "count";
    public static final String COUNT_NUM = "component.count_bolt_num";
    public static final int DEFAULT_SPOUT_NUM = 8;
    public static final int DEFAULT_SPLIT_BOLT_NUM = 4;
    public static final int DEFAULT_COUNT_BOLT_NUM = 4;

    public static String[] splitSentence(String sentence) {
        if (sentence != null) {
            return sentence.split("\\s+");
        }
        return null;
    }

    public StormTopology getTopology(Config config, TopoConfigReader reader) throws FileNotFoundException {

        // place holders
        final int spoutNum = reader.GetInstance("WordCount",SPOUT_ID);
        final int spBoltNum = reader.GetInstance("WordCount",SPLIT_ID);;
        final int cntBoltNum = reader.GetInstance("WordCount",COUNT_ID);;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, new FileReadSpout(), spoutNum);
        builder.setBolt(SPLIT_ID, new SplitSentence(), spBoltNum).shuffleGrouping(
                SPOUT_ID);
        builder.setBolt(COUNT_ID, new Count(), cntBoltNum).fieldsGrouping(SPLIT_ID,
                new Fields(SplitSentence.FIELDS));

        return builder.createTopology();
    }

    public class FileReadSpout extends BaseRichSpout {
        private org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(FileReadSpout.class);


        public static final String DEFAULT_FILE = "/resources/A_Tale_of_Two_City.txt";
        public static final boolean DEFAULT_ACK = true;
        public static final String FIELDS = "sentence";

        public final boolean ackEnabled;
        public final FileReader reader;
        private SpoutOutputCollector collector;

        private long count = 0;


        public FileReadSpout() throws FileNotFoundException {
            this(DEFAULT_ACK, DEFAULT_FILE);
        }

        public FileReadSpout(boolean ackEnabled) throws FileNotFoundException {
            this(ackEnabled, DEFAULT_FILE);
        }

        public FileReadSpout(boolean ackEnabled, String file) throws FileNotFoundException {
            this(ackEnabled, new FileReader(file));
        }

        public FileReadSpout(boolean ackEnabled, FileReader reader) {
            this.ackEnabled = ackEnabled;
            this.reader = reader;
        }

        @Override
        public void open(Map conf, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            if (ackEnabled) {
                collector.emit(new Values(reader.nextLine()), count);
                count++;
            } else {
                collector.emit(new Values(reader.nextLine()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class SplitSentence extends BaseBasicBolt {

        public static final String FIELDS = "word";

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            for (String word : splitSentence(input.getString(0))) {
                collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }

    }

    public static class Count extends BaseBasicBolt {
        public static final String FIELDS_WORD = "word";
        public static final String FIELDS_COUNT = "count";

        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_WORD, FIELDS_COUNT));
        }
    }
}
