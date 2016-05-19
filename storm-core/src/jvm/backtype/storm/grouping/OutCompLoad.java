package backtype.storm.grouping;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by anirudhnair on 5/12/16.
 */
public class OutCompLoad implements java.io.Serializable {
    private String sSrcCompID;
    private String sDestCompID;

    private HashMap<Integer,Double> taskLoad;

    public OutCompLoad(String src, String dest)
    {
        sSrcCompID = src;
        sDestCompID = dest;
        taskLoad = new HashMap<Integer,Double>();
    }

    public void AddTaskLoad(int task, double load)
    {
        taskLoad.put(task,load);
    }

    public double GetLoad(int task)
    {
        return taskLoad.get(task);
    }

    public HashMap<Integer,Double> GetTaskLoad()
    {
        return taskLoad;
    }

    public Set<Integer> GetTaskList()
    {
        return taskLoad.keySet();
    }

    public String SrcComp() { return sSrcCompID; }

    public String DestComp() { return sDestCompID; }

}
