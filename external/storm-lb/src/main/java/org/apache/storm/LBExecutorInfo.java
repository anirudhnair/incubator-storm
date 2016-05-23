package org.apache.storm;

import java.io.UnsupportedEncodingException;

/**
 * Created by anirudhnair on 5/20/16.
 */
public class LBExecutorInfo {

    private LBComponentInfo       m_oComp;
    private int                 m_nID; // for this senario we run with only one task on each
                                 // executor. So, executor and task can be used
                                 // interchangeably
    private int                 m_nBatchSize; // from zk
    private int                 m_nInterval;  // from zk
    private String              m_sHost;
    private ZookeeperClient     m_oZk;
    private LBTopologyInfo      m_oTopoInfo;
    private String              m_sTopoID;
    private String              m_sCompID;
    private Logger              m_oLogger;

    public LBExecutorInfo(LBComponentInfo comp, String host,  ZookeeperClient zk, int id, Logger oLogger)
    {
        m_oComp = comp;
        m_oZk   = zk;
        m_sHost = host;
        m_nID   = id;
        m_nBatchSize = 100;
        m_nInterval = 1;
        m_oTopoInfo = m_oComp.getTopoInfo();
        m_sTopoID = m_oTopoInfo.getTopoID();
        m_sCompID = m_oComp.getCompID();
        m_oLogger = oLogger;
    }


    public int UpdateQueueParmas()
    {
        String size_path  = "/" + Common.DYNAMIC_BATCHING_ROOT + "/"  + Common.DynamicBatchZnodeSize(m_sTopoID,
                m_sCompID,m_nID);
        String interval_path = "/" + Common.DYNAMIC_BATCHING_ROOT + "/"  + Common.DynamicBatchZnodeInterval(m_sTopoID,
                m_sCompID, m_nID);
        try {
            m_nBatchSize = Integer.parseInt(new String(m_oZk.GetData(size_path), "UTF-8"));
            m_nInterval = Integer.parseInt(new String(m_oZk.GetData(interval_path), "UTF-8"));
        } catch (UnsupportedEncodingException e)
        {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;
        }
        return Common.SUCCESS;
    }

    public int getID()
    {
        return m_nID;
    }

    public int getBatchSize()
    {
        return m_nBatchSize;
    }

    public int getFlushInterval()
    {
        return m_nInterval;
    }

    public String getHost()
    {
        return m_sHost;
    }



}
