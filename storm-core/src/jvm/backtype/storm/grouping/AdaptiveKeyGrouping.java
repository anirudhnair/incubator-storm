package backtype.storm.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by anirudhnair on 5/19/16.
 */
public class AdaptiveKeyGrouping implements CustomStreamGrouping, Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(AdaptiveKeyGrouping.class);
    private static final long serialVersionUID = -447379837314000353L;
    private List<Integer> targetTasks;
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
    private Fields fields = null;
    private Fields outFields = null;
    private final AtomicReference<Map<Integer,Double>> _probability = new AtomicReference<Map<Integer,Double>>(new HashMap<Integer,Double>());
    private Random random;

    public AdaptiveKeyGrouping() {
        //Empty
    }

    public AdaptiveKeyGrouping(Fields fields) {
        this.fields = fields;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        this.targetTasks = targetTasks;
        if (this.fields != null) {
            this.outFields = context.getComponentOutputFields(stream);
        }
        // initialize will equal probabilities for all tasks
        Map<Integer, Double> initProbMap = new HashMap<Integer, Double>();
        double initProb = 1.0/(double)targetTasks.size();
        for(Integer task: targetTasks)
        {
            initProbMap.put(task,initProb);
        }
        _probability.set(initProbMap);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        Map<Integer,Double> pd = _probability.get();
        List<Integer> boltIds = new ArrayList<Integer>(1);
        if (values.size() > 0) {
            byte[] raw = null;
            if (fields != null) {
                List<Object> selectedFields = outFields.select(fields, values);
                ByteBuffer out = ByteBuffer.allocate(selectedFields.size() * 4);
                for (Object o: selectedFields) {
                    if (o instanceof List) {
                        out.putInt(Arrays.deepHashCode(((List) o).toArray()));
                    } else if (o instanceof Object[]) {
                        out.putInt(Arrays.deepHashCode((Object[])o));
                    } else if (o instanceof byte[]) {
                        out.putInt(Arrays.hashCode((byte[]) o));
                    } else if (o instanceof short[]) {
                        out.putInt(Arrays.hashCode((short[]) o));
                    } else if (o instanceof int[]) {
                        out.putInt(Arrays.hashCode((int[]) o));
                    } else if (o instanceof long[]) {
                        out.putInt(Arrays.hashCode((long[]) o));
                    } else if (o instanceof char[]) {
                        out.putInt(Arrays.hashCode((char[]) o));
                    } else if (o instanceof float[]) {
                        out.putInt(Arrays.hashCode((float[]) o));
                    } else if (o instanceof double[]) {
                        out.putInt(Arrays.hashCode((double[]) o));
                    } else if (o instanceof boolean[]) {
                        out.putInt(Arrays.hashCode((boolean[]) o));
                    } else if (o != null) {
                        out.putInt(o.hashCode());
                    } else {
                        out.putInt(0);
                    }
                }
                raw = out.array();
            } else {
                raw = values.get(0).toString().getBytes(); // assume key is the first field
            }
            int firstChoice = (int) (Math.abs(h1.hashBytes(raw).asLong()) % this.targetTasks.size());
            int secondChoice = (int) (Math.abs(h2.hashBytes(raw).asLong()) % this.targetTasks.size());

            // here we look at the relative probabilites of the two choices and send the tuple in that ratio
            double firstChoiceRatio = pd.get(firstChoice);
            double secondChoiceRatio = pd.get(secondChoice);
            firstChoiceRatio = 10000*(firstChoiceRatio/ (firstChoiceRatio + secondChoiceRatio));
            secondChoiceRatio = 10000 * (secondChoiceRatio/ (firstChoiceRatio + secondChoiceRatio));
            int randomNo = random.nextInt(10000);
            if(randomNo < (int)firstChoiceRatio)
                boltIds.add(targetTasks.get(firstChoice));
            else
                boltIds.add(targetTasks.get(secondChoice));

        }
        return boltIds;
    }
}
