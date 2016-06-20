package org.apache.storm;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.TopologyInfo;

import java.util.ArrayList;

/**
 * Created by anirudhnair on 5/20/16.
 */
public class LBComponentInfo {

    private LBTopologyInfo      m_oTopoInfo;
    private String              m_sCompID;
    private boolean             m_bIsSpout;
    private int                 m_nTasks;
    // each component will have multiple executors
    public ArrayList<LBExecutorInfo>   m_lExecs;
    Logger                      m_oLogger;

    public LBComponentInfo(String compID, LBTopologyInfo topo, boolean isSpout, int nTasks, Logger logger)
    {
        m_sCompID       = compID;
        m_oTopoInfo     = topo;
        m_bIsSpout      = isSpout;
        m_nTasks        = nTasks;
        m_lExecs        = new ArrayList<LBExecutorInfo>();
        m_oLogger       = logger;
    }


    public int Init(TopologyInfo info, ZookeeperClient zk)
    {
        // get all the informaton of the component
        // go through all executors and look for component name
        int count = 0;
        for (ExecutorSummary exec: info.get_executors()) {
            String compID = exec.get_component_id();
            if(compID.equals(m_sCompID))
            {
                LBExecutorInfo lb_exec = new LBExecutorInfo(this,exec.get_host(),zk,exec.get_executor_info()
                        .get_task_start(),m_oLogger);
                m_lExecs.add(lb_exec);
                count++;
            }
        }
        if(count != m_nTasks)
        {
            m_oLogger.Error("The task count(" + Integer.toString(m_nTasks) + ") do not match with the executor count" +
                    "(" + Integer.toString(count) + ")" );
            return Common.FAILURE;
        }
        return Common.SUCCESS;
    }

    public LBTopologyInfo getTopoInfo()
    {
        return m_oTopoInfo;
    }

    public String getCompID()
    {
        return m_sCompID;
    }
}
