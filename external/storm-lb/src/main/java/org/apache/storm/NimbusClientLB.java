package org.apache.storm;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by anirudhnair on 10/11/16.
 */
public class NimbusClientLB {
    private Nimbus.Client nb;
    private Map m_mClusterConf;
    private Lock m_oLock;

    public NimbusClientLB()
    {
        m_mClusterConf = Utils.readStormConfig();
        m_oLock = new ReentrantLock();
        nb = backtype.storm.utils.NimbusClient.getConfiguredClient(m_mClusterConf).getClient();
    }

    public String submitJar(String localPath)
    {
        m_oLock.lock();
        String path = StormSubmitter.submitJar(m_mClusterConf, localPath);
        m_oLock.unlock();
        return path;
    }

    public void kill(String name, KillOptions opts) throws Exception {
        m_oLock.lock();
        nb.killTopologyWithOpts(name, opts);
        m_oLock.unlock();
    }

    public void submitTopology(String a,String b,String c, StormTopology d) throws Exception {
        m_oLock.lock();
        nb.submitTopology(a, b, c, d);
        m_oLock.unlock();
    }

    public ClusterSummary getClusterInfo() throws Exception {
        m_oLock.lock();
        ClusterSummary summary = nb.getClusterInfo();
        m_oLock.unlock();
        return summary;
    }

    public TopologyInfo getTopologyInfo(String id) throws Exception {
        m_oLock.lock();
        TopologyInfo info = nb.getTopologyInfo(id);
        m_oLock.unlock();
        return info;
    }

    public StormTopology getUserTopology(String id) throws Exception
    {
        m_oLock.lock();
        StormTopology topo = nb.getUserTopology(id);
        m_oLock.unlock();
        return topo;
    }
}
