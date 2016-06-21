package backtype.storm.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by anirudhnair on 2/15/16.
 */
public class BatchingShuffleLBGrouping implements CustomStreamGrouping, Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(BatchingShuffleLBGrouping.class);
    private final AtomicReference<Map<Integer,Double>> _probability = new AtomicReference<Map<Integer,Double>>(new HashMap<Integer,Double>());
    private Random random;
    private List<Integer>[] rets; //return value of the chooseTasks requires a list
    private int[] targets; // copy the targets from the prepare method here


    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        rets = (List<Integer>[])new List<?>[targetTasks.size()];
        targets = new int[targetTasks.size()];
        for (int i = 0; i < targets.length; i++) {
            rets[i] = Arrays.asList(targetTasks.get(i));
            targets[i] = targetTasks.get(i);
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

    public void setProbDist(Map<Integer,Double> newProb)
    {
        if(newProb != null)
            _probability.set(new HashMap<Integer, Double>(newProb));
    }

    public Map<Integer,Double> getProbDist()
    {
        return _probability.get();
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        // get the probability distribution
        Map<Integer,Double> pd = _probability.get();
        int sum = 0;
        int randomNo = random.nextInt(10000);
        for(int i=0;i < targets.length; ++i)
        {
            sum += (int)(pd.get(targets[i]).doubleValue()*10000.0);
            if(sum > randomNo)
            {
                return rets[i];
            }
        }
        return rets[rets.length - 1];
    }
}
