package org.apache.storm;

import backtype.storm.generated.Nimbus;

/**
 * Created by anirudhnair on 6/14/16.
 */
public class DataRateAwareLB extends AbstLoadBalance {

    @Override
    public int Init(StatsCollector oCollect, String sTopoName, ZookeeperClient zk_client, LBConfigReader
            oConfig, Logger oLogger,NimbusClientLB nimbus_client)
    {
        super.Initialize(Common.LOAD_BALANCER.DATA_RATE_AWARE, sTopoName,oCollect, zk_client, oConfig, oLogger, nimbus_client);
        return Common.SUCCESS;
    }


    @Override
    public void load_balance() {
        m_oLogger.Info("DataRateAwareLB begins with params: " +  PrintParams());
        try {
            Thread.sleep(m_nStartAfter);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        }

        // init
        BATCH_ACTION prev_batch_action = BATCH_ACTION.NONE;
        double diff_threshold_min = Double.parseDouble(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType),
                "diff_threshold_inc"));
        double diff_threshold_inc = diff_threshold_min;
        double diff_threshold_dec = 0.9*diff_threshold_inc;
        // (m_oLBType),
                //"diff_threshold_dec"));
        int curr_batch_size = 1; // job always starts with size 1
        int curr_update_value = 0;
        int nStatReadings = (int)(m_nLBPeriod/m_nStatCollectionInterval);
        m_oLogger.Info("Stats Reading: " + Integer.toString(nStatReadings));
        long iter = 0;
        while(true)
        {
            iter++;
            //double acked_rate = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgAckCount(nStatReadings); // last 20
            // secs
            //double data_rate = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgEmitCount(nStatReadings);

            long acked = m_oStatsCollector.GetTopoStat(m_sTopoName).TotalAcked();
            long emited = m_oStatsCollector.GetTopoStat(m_sTopoName).TotalEmitted();
            double data_rate = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgEmitCount(nStatReadings);
            double throughput = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgAckCount(nStatReadings);
            long diff = emited - acked;
            boolean bSLA = sla_satisfied();
            if( diff > diff_threshold_inc && bSLA)
            {
                if (prev_batch_action == BATCH_ACTION.INC) {
                    curr_update_value = Math.min(curr_update_value + 1, 7);
                    curr_batch_size = Math.min(130, curr_batch_size + Common.mapInttoPower.get(curr_update_value));

                } else // prev action is either none or dec
                {
                    curr_update_value = 0;
                    curr_batch_size = Math.min(130, curr_batch_size + Common.mapInttoPower.get(curr_update_value));
                }
                diff_threshold_inc = diff*1.1;
                diff_threshold_dec = 0.9*diff_threshold_inc;
                prev_batch_action = BATCH_ACTION.INC;
                update_bacth_size_all(curr_batch_size);

            } else if (diff < diff_threshold_dec)
            {
                if (prev_batch_action == BATCH_ACTION.DEC) {
                    curr_update_value = Math.min(curr_update_value + 1, 7);
                    // the current batch size is decreased
                    curr_batch_size = Math.max(1, curr_batch_size - Common.mapInttoPower.get(curr_update_value));
                } else // either none or inc action
                {
                    curr_update_value = 0;
                    // the current batch size is decreased
                    curr_batch_size = Math.max(1, curr_batch_size - Common.mapInttoPower.get(curr_update_value));
                }
                diff_threshold_inc = Math.max(diff_threshold_min,diff_threshold_inc*0.95);
                diff_threshold_dec = 0.95*diff_threshold_inc;
                prev_batch_action = BATCH_ACTION.DEC;
                update_bacth_size_all(curr_batch_size);
            } else
            {
                prev_batch_action = BATCH_ACTION.NONE;
            }
            String log = " Iter: " + Long.toString(iter) +
                    " DataRate: " + Double.toString(data_rate) +
                    " Throughput: " + Double.toString(throughput) +
                    " AckCount: " + Double.toString(acked)  +
                    " EmitCount: " + Double.toString(emited)  +
                    " Diff: " + Double.toString(diff)  +
                    " SLA: " + Boolean.toString(bSLA) +
                    " BATCH_ACTION: " + mapBatchActionToString.get(prev_batch_action) +
                    " UPDATE_VALUE: " + Long.toString(curr_update_value) +
                    " BATCH_SIZE: " + Long.toString(curr_batch_size) +
                    " Threshold_Inc: " + Double.toString(diff_threshold_inc) +
                    " Threshold_Dec: " + Double.toString(diff_threshold_dec);
            m_oLogger.Info(log);
            System.out.println(log);
            try {
                Thread.sleep(m_nLBPeriod);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

}
