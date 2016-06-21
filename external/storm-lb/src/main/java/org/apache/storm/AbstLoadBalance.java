package org.apache.storm;

import backtype.storm.generated.Nimbus;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by anirudhnair on 6/14/16.
 */
abstract public class AbstLoadBalance implements Runnable{

    protected Common.LOAD_BALANCER             m_oLBType;
    protected String                           m_sTopoName;
    protected long                             m_nLBPeriod;
    protected long                             m_nStartAfter;
    protected long                             m_nSLA;
    protected long                             m_nStatCollectionInterval;
    protected int                              m_nStatPeriod;
    protected StatsCollector                   m_oStatsCollector;
    protected ZookeeperClient                  m_oZK;
    protected Logger                           m_oLogger;
    protected LBConfigReader                   m_oConfig;
    protected LBTopologyInfo                   m_oTopoInfo;
    protected Nimbus.Client                    m_oNimbusClient;
    protected Common.SLA_TYPE                  m_oSLAType;

    public static enum BATCH_ACTION {INC, DEC, NONE};
    public static final Map<BATCH_ACTION, String> mapBatchActionToString = Collections.unmodifiableMap(
            new HashMap<BATCH_ACTION, String>() {{
                put(BATCH_ACTION.DEC,"Decrement");
                put(BATCH_ACTION.INC,"Increment");
                put(BATCH_ACTION.NONE, "None");
            }});

    public static final Map<String, BATCH_ACTION> mapStringToBatchAction = Collections.unmodifiableMap(
            new HashMap<String, BATCH_ACTION>() {{
                put("Decrement", BATCH_ACTION.DEC);
                put("Increment",BATCH_ACTION.INC);
                put("None", BATCH_ACTION.NONE);
            }});



    public int Initialize(Common.LOAD_BALANCER type, String sTopoName,StatsCollector oCollect, ZookeeperClient
            zk_client,
                    LBConfigReader oConfig, Logger oLogger, Nimbus.Client nimbus_client)
    {
        m_oLBType = type;
        m_sTopoName = sTopoName;
        m_oZK = zk_client;
        m_oNimbusClient = nimbus_client;
        m_oTopoInfo = new LBTopologyInfo(m_sTopoName,oLogger,m_oZK);
        m_oTopoInfo.Init(m_oNimbusClient);
        m_oStatsCollector = oCollect;
        m_oLogger = oLogger;
        m_oConfig = oConfig;
        m_nLBPeriod = Long.parseLong(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType), "period"));
        m_nStartAfter = Long.parseLong(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType), "start_after"));
        m_nSLA = Long.parseLong(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType), "sla"));
        m_nStatCollectionInterval = Long.parseLong(m_oConfig.GetValue("STAT_COLLECTION", "interval"));
        m_nStatPeriod = Integer.parseInt(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType), "stat_period"));
        m_oSLAType = Common.mapSLAStrToType.get(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType), "sla_type"));
        return Common.SUCCESS;
    }

    public String PrintParams()
    {
        String sParams = " Topology:" + m_sTopoName +
                " Period:" + Long.toString(m_nLBPeriod) +
                " StartAfter:" + Long.toString(m_nStartAfter) +
                " SLA:" + Long.toString(m_nSLA) +
                " Stats Period:" + Integer.toString(m_nStatPeriod) +
                " SLA Type:" + Common.mapSLATypeToStr.get(m_oSLAType);

        return sParams;

    }

    public boolean sla_satisfied()
    {
        Histogram hist_ = m_oStatsCollector.GetTopoStat(m_sTopoName).GetLatencyHist();
        long latency_value_ms = 0;
        switch(m_oSLAType) {
            case LATENCY_99:
                latency_value_ms = hist_.getValueAtPercentile(99.0)/1000000;
                break;
            case LATENCY_999:
                latency_value_ms = hist_.getValueAtPercentile(99.9)/1000000;
                break;
            case LATENCY_MEAN:
                latency_value_ms = (long) (hist_.getMean()/1000000);
                break;
        }

        if(latency_value_ms > m_nSLA) {
            m_oLogger.Info("SLA Satisfaction Test: False " + Long.toString(latency_value_ms) + " > " + Long.toString
                    (m_nSLA));
            return false;
        }
        else {
            m_oLogger.Info("SLA Satisfaction Test: True " + Long.toString(latency_value_ms) + " <= " + Long.toString
                    (m_nSLA));
            return true;
        }
    }

    public int update_bacth_size_all(int batch_size)
    {
        ArrayList<LBExecutorInfo> all_execs = new ArrayList<LBExecutorInfo>();
        all_execs.addAll(m_oTopoInfo.GetSpoutInfo().m_lExecs);
        for(LBComponentInfo info: m_oTopoInfo.GetBolts().values())
            all_execs.addAll(info.m_lExecs);
        for(LBExecutorInfo info: all_execs)
        {
            if( Common.FAILURE == info.setBatchSize(batch_size))
            {
                m_oLogger.Error("Failed to set batch_size on " + Integer.toString(info.getID()));
            }
        }
        return Common.SUCCESS;
    }

    abstract public void load_balance();
    abstract public int Init(StatsCollector oCollect, String sTopoName, ZookeeperClient zk_client, LBConfigReader
            oConfig, Logger oLogger,Nimbus.Client nimbus_client);

    @Override
    public void run() {
        load_balance();
    }

}
