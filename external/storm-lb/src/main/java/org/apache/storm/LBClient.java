package org.apache.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by anirudhnair on 3/4/16.
 */
public class LBClient {

    private StatsCollector                  m_oCollector;
    private Logger                          m_oLogger;
    private Nimbus.Client                   m_oNimbus;
    private ZookeeperClient                 m_oZK;
    private Map                             m_mClusterConf;
    public int Initialize(String zkHost, Logger oLogger)
    {
        m_oLogger = oLogger;
        m_mClusterConf = Utils.readStormConfig();
        m_oNimbus = NimbusClient.getConfiguredClient(m_mClusterConf).getClient();
        // init stat collector
        m_oCollector = new StatsCollector();
        try {
            m_oCollector.Initialize(m_oNimbus,m_oLogger);
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
        }

        // start the node stat collection
        m_oCollector.StartNodeStatCollection(Common.STAT_COLLECTION_INTERVAL);

        // connect to zookeeper
        m_oZK = new ZookeeperClient(zkHost,m_oLogger);
        if( m_oZK.Connect() == Common.FAILURE)
        {
            m_oLogger.Error("Exitign LB Cleint Init due to ZK connection problem");
            return Common.FAILURE;
        }
        return Common.SUCCESS;
    }

    public void kill(String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        m_oNimbus.killTopologyWithOpts(name, opts);
    }

    public int SubmitTopology(String sTopoName, Config conf, StormTopology topology, Common.LOAD_BALANCERS lb) throws Exception
    {
        m_oLogger.Info("New topology submission in progress: " + sTopoName);
        m_oCollector.AddTopologyToStatCollection(sTopoName, conf);
        try {
            StormSubmitter.submitTopology(sTopoName, conf, topology);
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
        } finally {
            kill(sTopoName);
        }

        // get topoinfo from nimbus
        LBTopologyInfo topoInfo = new LBTopologyInfo(sTopoName,m_oLogger,m_oZK);
        // do sone load balancig things here
        // use LBTopologyInfo and StatsCollector to get great things done
        m_oLogger.Info("Topology submitted: " + sTopoName);
        return Common.SUCCESS;
    }

    public int StopLB(String sTopoName)
    {
        return Common.SUCCESS;
    }

    public void StopStatCollection(String sTopoName)
    {

    }

    public NodeStat GetNodeStat(String IP)
    {
        NodeStat nodeStat = m_oCollector.GetNodeStat(IP);
        return nodeStat;
    }

    public TopologyStat GetTopologyStat(String sTopoName)
    {
        TopologyStat topoStat = m_oCollector.GetTopoStat(sTopoName);
        return topoStat;
    }
}
