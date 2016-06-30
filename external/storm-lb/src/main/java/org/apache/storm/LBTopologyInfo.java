package org.apache.storm;

import backtype.storm.generated.*;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by anirudhnair on 5/20/16.
 */
public class LBTopologyInfo {

    private String          m_sTopoName;
    private String          m_sTopoID;
    private TopologyInfo    m_oTopoInfoStorm;
    private StormTopology   m_oStormTopology;
    private Logger          m_oLogger;
    private LBComponentInfo m_oSpout;
    private HashMap<String,LBComponentInfo> m_lBolts;
    private ZookeeperClient m_oZK;

    public LBComponentInfo GetSpoutInfo() { return m_oSpout;}

    public LBComponentInfo GetBoltInfo(String compName) {
        return m_lBolts.get(compName);
    }

    public HashMap<String,LBComponentInfo> GetBolts()
    {
        return m_lBolts;
    }

    public LBTopologyInfo(String sTopoName, Logger oLogger, ZookeeperClient zk)
    {
        m_sTopoName = sTopoName;
        m_oLogger   = oLogger;
        m_sTopoID   = null;
        m_lBolts    = new HashMap<String,LBComponentInfo>();
        m_oZK       = zk;
    }

    public int Init(Nimbus.Client client)
    {
        ClusterSummary summary = null;
        try {
            summary = client.getClusterInfo();
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;
        }


        for (TopologySummary ts: summary.get_topologies()) {
            if (m_sTopoName.equals(ts.get_name())) {
                m_sTopoID = ts.get_id();
            }
        }

        if(m_sTopoID == null)
        {
            m_oLogger.Error("Unable to find topology - " + m_sTopoName + " in the cluster summary from nimbus");
            return Common.FAILURE;
        }


        try {
            m_oTopoInfoStorm = client.getTopologyInfo(m_sTopoID);
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;

        }

        try {
            m_oStormTopology = client.getUserTopology(m_sTopoID);
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;
        }

        // bolt comp names
        Set<String> bolts = m_oStormTopology.get_bolts().keySet();

        // fill spout information
        SpoutSpec spoutSpec = m_oStormTopology.get_spouts().get("spout");
        int nSpoutTasks = spoutSpec.get_common().get_parallelism_hint();
        m_oSpout = new LBComponentInfo("spout",this,true,nSpoutTasks,m_oLogger);
        m_oSpout.Init(m_oTopoInfoStorm,m_oZK);

        //fill bolt informations
        for(String compName: bolts)
        {
            Bolt boltSpec = m_oStormTopology.get_bolts().get(compName);
            int nBoltTask = boltSpec.get_common().get_parallelism_hint();
            LBComponentInfo boltInfo = new LBComponentInfo(compName,this,false,nBoltTask,m_oLogger);
            boltInfo.Init(m_oTopoInfoStorm,m_oZK);
            m_lBolts.put(compName,boltInfo);
        }
        return Common.SUCCESS;
    }

    public String getTopoName()
    {
        return m_sTopoName;
    }

    public String  getTopoID()
    {
        return m_sTopoID;
    }
}
