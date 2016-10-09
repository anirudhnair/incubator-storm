package org.apache.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.json.simple.JSONValue;

import java.util.List;
import java.util.Map;

import static backtype.storm.Config.STORM_ZOOKEEPER_SERVERS;

/**
 * Created by anirudhnair on 3/4/16.
 */
public class LBClient {

    private StatsCollector                  m_oCollector;
    private Logger                          m_oLogger;
    private Nimbus.Client                   m_oNimbus;
    private ZookeeperClient                 m_oZK;
    private Map                             m_mClusterConf;
    private TopoConfigReader                m_oTopoConfig;
    private LBConfigReader                  m_oLBConfig;
    private String                          m_sJarLocationNimbus;

    public int Initialize(String zkHost, Logger oLogger, String sTopoConf, String sLBPath, String sJarPathLocal) throws Exception {
        m_oLogger = oLogger;
        m_mClusterConf = Utils.readStormConfig();
        m_oNimbus = NimbusClient.getConfiguredClient(m_mClusterConf).getClient();
        m_oTopoConfig = new TopoConfigReader(sTopoConf);
        m_oTopoConfig.Init();
        m_oLBConfig = new LBConfigReader(sLBPath);
        m_oLBConfig.Init();
        // init stat collector
        m_oCollector = new StatsCollector();
        try {
            m_oCollector.Initialize(m_oNimbus,m_oLogger);
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;
        }

        // start the node stat collection
        m_oCollector.StartNodeStatCollection(Long.parseLong(m_oLBConfig.GetValue("STAT_COLLECTION", "interval")));

        // start topology stat collection
        m_oCollector.StartTopologyStatCollection(Long.parseLong(m_oLBConfig.GetValue("STAT_COLLECTION", "interval")));

        // connect to zookeeper
        m_oZK = new ZookeeperClient(zkHost,m_oLogger);
        if( m_oZK.Connect() == Common.FAILURE)
        {
            m_oLogger.Error("Exiting LB Client Init due to ZK connection problem");
            return Common.FAILURE;
        }

        // submit jar to nimbus
        m_sJarLocationNimbus = StormSubmitter.submitJar(m_mClusterConf,sJarPathLocal);
        return Common.SUCCESS;
    }

    public void kill(String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        m_oNimbus.killTopologyWithOpts(name, opts);
    }

    public int SubmitTopology(String sTopoName, Config conf, StormTopology topology, Common.LOAD_BALANCER lb) throws Exception
    {
        m_oLogger.Info("New topology submission in progress: " + sTopoName);
        conf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
                "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled");
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

        m_oCollector.AddTopologyToStatCollection(sTopoName, conf);
        try {
            String jsonConf = JSONValue.toJSONString(conf);
            m_oNimbus.submitTopology(sTopoName, m_sJarLocationNimbus,jsonConf, topology);
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            kill(sTopoName);
        }


        m_oLogger.Info("Topology submitted: " + sTopoName);

        Thread.sleep(2000);
        // get topoinfo from nimbus
        if( lb != Common.LOAD_BALANCER.NONE)
        {
            m_oLogger.Info("Starting loadbalancer");
            AbstLoadBalance loadBalancer = null;
            switch(lb) {
                case DATA_RATE_AWARE:
                    loadBalancer = new DataRateAwareLB();
                    break;
                case ENERGY_AWARE:
                    loadBalancer = new EnergyAwareLB();
                    break;
                case LOAD_AWARE:
                    loadBalancer = new LoadAwareLB();
                    break;
            }
            if( Common.FAILURE == loadBalancer.Init(m_oCollector,sTopoName,m_oZK,m_oLBConfig,m_oLogger,m_oNimbus))
            {
                m_oLogger.Error("Error initializing load-balancer");
                return Common.FAILURE;
            }
            Thread lb_thread = new Thread(loadBalancer);
            lb_thread.start();
            m_oLogger.Info("Loadbalancer successfully started");
        }

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
