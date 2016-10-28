package org.apache.storm;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.metric.HttpForwardingMetricsServer;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.HdrHistogram.Histogram;
import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by anirudhnair on 3/4/16.
 */
public class StatsCollector {



    class HttpMetricsServerImpl extends HttpForwardingMetricsServer
    {
        private TopologyStat m_oTopoStat;

        public HttpMetricsServerImpl(Map conf, TopologyStat topoStat) {
            super(conf);
            m_oTopoStat = topoStat;
        }

        @Override
        public void handle(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> dataPoints) {

            for (IMetricsConsumer.DataPoint dp: dataPoints) {
                if ("comp-lat-histo".equals(dp.name) && dp.value instanceof Histogram) {
                    m_oTopoStat.AddLatency((Histogram)dp.value);
                }
            }
        }
    }

    private NimbusClientLB                  m_oNimbus;
    private NimbusClientLB                  m_oTopoStatNimbus;
    private Thread 					        m_oNodeStatThread;
    private Thread                          m_oTopoStatThread;
    private ArrayList<String>               m_lNodes; //list of worker nodes in the cluster
    private ArrayList<String>               m_lTopologyNames; // topologies for which the stats need to be collected
    private Lock                            m_topo_list_lock;
    private Map<String, NodeStatClient>     m_mNodetoStatClient;
    private Logger                          m_oLogger;
    private Map<String,NodeStat>            m_mNodetoStat;
    private Map<String,TopologyStat>        m_mTopotoStat;
    private Map<String,HttpMetricsServerImpl> m_mTopotoServer;

    public void Wait(long millisec)
    {

        try {
            m_oNodeStatThread.join(millisec);
            m_oTopoStatThread.join(millisec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int Initialize(NimbusClientLB oClient, NimbusClientLB oNodeStatNimbus, Logger logger) throws Exception
    {
        m_oLogger = logger;
        //instantiate
        m_lNodes = new ArrayList<>();
        m_mNodetoStat = new HashMap<>();
        m_mTopotoStat = new HashMap<>();
        m_lTopologyNames = new ArrayList<>();
        m_mNodetoStatClient = new HashMap<>();
        m_mTopotoServer = new HashMap<>();
        m_topo_list_lock = new ReentrantLock();


        m_oNimbus = oClient;
        m_oTopoStatNimbus = oNodeStatNimbus;
        // get the nodelist
        List<SupervisorSummary> superviosrList = m_oNimbus.getClusterInfo().get_supervisors();
        for(SupervisorSummary sup: superviosrList)
        {
            m_oLogger.Info("Host added" + sup.get_host());
            m_lNodes.add(sup.get_host());
        }

        // create the statclient for all the nodes. the stat server should be stared in al the nodes
        for(String nodeIp: m_lNodes)
        {
            NodeStatClient client = new NodeStatClient();
            if( Common.FAILURE == client.Init(nodeIp,m_oLogger))
            {
                m_oLogger.Error("Unable to create client to the Stats Server on " + nodeIp);
                System.exit(1);
            }
            m_mNodetoStatClient.put(nodeIp, client);
            m_mNodetoStat.put(nodeIp,new NodeStat());
        }



        return Common.SUCCESS;
    }

    private void GetNodeStatFromTopologyInfo()
    {


        //m_oLogger.Info("GetNodeStatFromTopologyInfo: Get Node stats from topology information");
        Map<String,Long> iptoMsgCount = new HashMap<>();
        for(String nodeIp: m_lNodes)
        {
            //m_oLogger.Info("GetNodeStatFromTopologyInfo: Adding node to list " + nodeIp);
            iptoMsgCount.put(nodeIp, (long)0);
        }
        ClusterSummary summary = null;
        try {
            summary = m_oNimbus.getClusterInfo();
        } catch (Exception e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return;
        }
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            id = ts.get_id();
            TopologyInfo info = null;
            try {
                info = m_oNimbus.getTopologyInfo(id);
            } catch (Exception e) {
                m_oLogger.Error(m_oLogger.StackTraceToString(e));
                continue;
            }
            for (ExecutorSummary exec: info.get_executors()) {
                if("__eventlogger".equals(exec.get_component_id()) ||
                        "__metricsbacktype.storm.metric.LoggingMetricsConsumer".equals(exec.get_component_id()) ||
                        "__metricsbacktype.storm.metric.HttpForwardingMetricsConsumer".equals(exec.get_component_id())) continue;
                String sHost = exec.get_host();
                // get transffered count
                long msg_count = 0;
                Map<String,Long> all_time_stat_common = exec.get_stats().get_transferred().get(":all-time");
                for(Long count: all_time_stat_common.values())
                {
                    msg_count+=count;
                }
                // if bolt get executed count
                if(exec.get_stats().get_specific().is_set_bolt())
                {
                    Map<GlobalStreamId, Long> all_time_bolt = exec.get_stats().get_specific().get_bolt().
                            get_executed().get(":all-time");
                    for(Long count: all_time_bolt.values())
                    {
                        msg_count+=count;
                    }
                }
                //m_oLogger.Info("GetNodeStatFromTopologyInfo: " + sHost + " " + id + " " + exec.get_component_id()  +
                  //      " " + Long.toString(msg_count));
                Long curr_count = iptoMsgCount.get(sHost);
                iptoMsgCount.put(sHost,curr_count+msg_count);
            }

        }

        for (Map.Entry<String, Long> entry : iptoMsgCount.entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();
            //m_oLogger.Info("Node Stat: Node-" + key + " Msg Count: " + value.toString());
            m_mNodetoStat.get(key).AddMessageCount(value);
        }

    }

    public int StartNodeStatCollection(final long time_ms)
    {
        m_oNodeStatThread = new Thread(new Runnable() {
            public void run() {
                // each node get mem and cpu info
                long counter = 0;
                while(true) {
                    try {
                        Thread.sleep(time_ms);
                    } catch (InterruptedException e1) {
                        m_oLogger.Error(m_oLogger.StackTraceToString(e1));
                    }
                    counter++;
                    m_oLogger.Info("Node Stat Collection " + Long.toString(counter));

                    for (String nodeIp : m_lNodes) {
                        try {
                            double cpuUsage = m_mNodetoStatClient.get(nodeIp).CPUUsage();
                            double memUsage = m_mNodetoStatClient.get(nodeIp).MemUsage();
                            double powerUsage = m_mNodetoStatClient.get(nodeIp).PowerUsgae();
                            m_mNodetoStat.get(nodeIp).AddCPUUsage((float) cpuUsage);
                            m_mNodetoStat.get(nodeIp).AddMemUsgae((float) memUsage);
                            m_mNodetoStat.get(nodeIp).AddPowerUsage((float) powerUsage);
                            //m_oLogger.Info("Node Stat: Node-" + nodeIp + " CPU-" + Double.toString(cpuUsage) +
                                   // " Mem-" + Double.toString(memUsage) + " Power-" + Double.toString(powerUsage));
                        } catch (Exception e) {
                            m_oLogger.Error("NodeStatCollection: Error getting stats from " + nodeIp);
                            m_oLogger.Error(m_oLogger.StackTraceToString(e));
                        }
                    }

                    // from each topology get the total messages in each node
                    try {
                        GetNodeStatFromTopologyInfo();
                    } catch (Exception e) {
                        m_oLogger.Error(m_oLogger.StackTraceToString(e));
                    }

                    if(counter%5 == 0) //every 100 readings, record the node statistics
                    {
                        for(String sIp: m_lNodes)
                        {
                            m_oLogger.Info("NodeStatReport: " +  Long.toString(System.currentTimeMillis()) + " " + sIp + " " +
                                    m_mNodetoStat.get(sIp).PrintStatus());
                        }
                    }
                }

            }
        });
        m_oNodeStatThread.start();
        return Common.SUCCESS;
    }


    /*
    *   Topology stat collection is for acked tupls and failed tuples count.
    *   For latency there is a storm metrics way of collecting ata using a httpserver
     */
    public int StartTopologyStatCollection(final long time_ms)
    {
        m_oTopoStatThread = new Thread(new Runnable() {
            public void run() {
                long counter = 0;
                while(true) {
                    try {
                        Thread.sleep(time_ms);
                    } catch (InterruptedException e1) {
                        m_oLogger.Error(m_oLogger.StackTraceToString(e1));
                    }
                    counter++;
                    // lock the topology list since topology can be added any time
                    ClusterSummary summary = null;
                    try {
                        summary = m_oTopoStatNimbus.getClusterInfo();
                    } catch (Exception e) {
                        m_oLogger.Error(m_oLogger.StackTraceToString(e));
                        continue;
                    }
                    m_topo_list_lock.lock();
                    // go through the topology list and get stats for each topology
                    for( String sTopoName: m_lTopologyNames) {
                        String id = null;
                        for (TopologySummary ts: summary.get_topologies()) {
                            if (sTopoName.equals(ts.get_name())) {
                                id = ts.get_id();
                            }
                        }

                        if(id == null)
                        {
                            m_oLogger.Error("Unable to find topology - " + sTopoName + " in the cluster summary from nimbus");
                            continue;
                        }

                        TopologyInfo info = null;
                        try {
                            info = m_oTopoStatNimbus.getTopologyInfo(id);
                        } catch (Exception e) {
                            m_oLogger.Error(m_oLogger.StackTraceToString(e));

                        }
                        if(info == null)
                        {
                            m_oLogger.Error("Topology info returned null for " + sTopoName);
                            continue;
                        }
                        long acked = 0;
                        long failed = 0;
                        long emitted = 0;
                        List<ExecutorSummary> execs = info.get_executors();
                        if(execs == null || execs.isEmpty())
                        {
                            m_oLogger.Error("Executor info returned null or empty for topology " + sTopoName);
                            continue;
                        }
                        SpoutStats stats = null;
                        ExecutorStats exec_stats = null;
                        ExecutorSummary spout_summary = null;
                        for (ExecutorSummary exec: execs) {
                            if ("spout".equals(exec.get_component_id())) {
                                spout_summary = exec;
                                exec_stats = exec.get_stats();
                                if (exec_stats == null) {
                                    m_oLogger.Error("Executor stats returned null or empty");
                                    break;
                                }
                                stats = exec_stats.get_specific().get_spout();
                                if (stats == null) {
                                    m_oLogger.Error("Executor spout stats returned null or empty");
                                    break;
                                }
                            }
                        }

                        if(stats != null && spout_summary.get_stats() != null) {
                            Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                            Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                            Map<String, Long> emittedMap = spout_summary.get_stats().get_emitted().get(":all-time");
                            if (ackedMap != null && emittedMap != null) {
                                for (String key : ackedMap.keySet()) {
                                    if ("default".equals(key)) {
                                        if (failedMap != null) {
                                            Long tmp = failedMap.get(key);
                                            if (tmp != null) {
                                                failed += tmp;
                                            }
                                        }
                                        long ackVal = ackedMap.get(key);
                                        acked += ackVal;
                                        long emitVal = emittedMap.get(key);
                                        emitted += emitVal;
                                        break;
                                    }
                                }
                            }
                        }

                        m_mTopotoStat.get(sTopoName).UpdateAckedCount(acked);
                        m_mTopotoStat.get(sTopoName).UpdateFailedCount(failed);
                        m_mTopotoStat.get(sTopoName).UpdateEmitCount(emitted);
                        m_oLogger.Info("Topology Stat: " + sTopoName + " Count: " + counter + " Acked: " + Long
                                .toString(acked) +
                                " Failed: " + Long.toString(failed) + " Emitted: " + Long.toString(emitted));



                    }

                    if(counter%5 == 0) //every 100 readings, record the node statistics
                    {
                        for( String sTopoName: m_lTopologyNames)
                        {
                            m_oLogger.Info("TopoStatReport: " +  Long.toString(System.currentTimeMillis()) + " " +
                                    sTopoName + " " + m_mTopotoStat.get(sTopoName).PrintStatus());
                        }
                    }



                    m_topo_list_lock.unlock();

                }

            }
        });
        m_oTopoStatThread.start();
        return Common.SUCCESS;
    }

    /*
    add topology name to list and also setup the httpserver
     */
    public int AddTopologyToStatCollection( String topo_name,  Config conf) {

        m_topo_list_lock.lock();
        m_oLogger.Info("Adding " + topo_name + " to stats collection list");
        m_lTopologyNames.add(topo_name);
        // create topostat
        TopologyStat stat = new TopologyStat();
        m_mTopotoStat.put(topo_name, stat);
        // create server
        HttpMetricsServerImpl server = new HttpMetricsServerImpl(conf, stat);
        m_mTopotoServer.put(topo_name, server);
        server.serve();
        String url = server.getUrl();
        conf.registerMetricsConsumer(backtype.storm.metric.LoggingMetricsConsumer.class);
        conf.registerMetricsConsumer(backtype.storm.metric.HttpForwardingMetricsConsumer.class, url, 1);
        m_oLogger.Info("Created http server to receive stats from the topology");
        m_topo_list_lock.unlock();
        return Common.SUCCESS;

    }

    public ArrayList<String> GetNodeList()
    {
        return m_lNodes;
    }

    public NodeStat GetNodeStat(String sIP)
    {
        NodeStat nodeStat = null;
        nodeStat = m_mNodetoStat.get(sIP);
        return nodeStat;
    }

    public TopologyStat GetTopoStat(String sTopoName)
    {
        TopologyStat topoStat = null;
        topoStat = m_mTopotoStat.get(sTopoName);
        return topoStat;
    }

    public static void main( String[] args ) throws Exception {
        System.out.println( "Hello World!" );
        Map clusterConf = Utils.readStormConfig();
        Nimbus.Client oNimbus = NimbusClient.getConfiguredClient(clusterConf).getClient();
        ClusterSummary summary = oNimbus.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            id = ts.get_id();
            TopologyInfo info = oNimbus.getTopologyInfo(id);
            for (ExecutorSummary exec: info.get_executors()) {

            }

        }

    }

}

