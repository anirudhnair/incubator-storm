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
        double diff_threshold_inc = Double.parseDouble(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType),
                "diff_threshold_inc"));
        double diff_threshold_dec = Double.parseDouble(m_oConfig.GetValue(Common.mapLBTypeToName.get(m_oLBType),
                "diff_threshold_dec"));
        int curr_batch_size = 1;
        int curr_update_value = 0;
        int nStatReadings = (int)(m_nStatPeriod/m_nStatCollectionInterval);
        long iter = 0;
        while(true)
        {
            iter++;
            double acked_rate = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgAckCount(nStatReadings); // last 20 secs
            double data_rate = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgEmitCount(nStatReadings);

            if( Math.abs(data_rate - acked_rate) > diff_threshold_dec)
            {
                if (data_rate > acked_rate && sla_satisfied()) {
                    // there is a need to increase the batch size to
                    // maintain the stability of the system
                    if (prev_batch_action == BATCH_ACTION.INC) {
                        curr_update_value++;
                        curr_batch_size += Common.mapInttoPower.get(curr_update_value);
                        update_bacth_size_all(curr_batch_size);

                    } else // prev action is either none or dec
                    {
                        curr_update_value = 0;
                        curr_batch_size += Common.mapInttoPower.get(curr_update_value);
                        update_bacth_size_all(curr_batch_size);
                    }

                    prev_batch_action = BATCH_ACTION.INC;

                } else if (data_rate < acked_rate)
                // chance to decrease the batch size and increase
                // the latency
                {
                    if (prev_batch_action == BATCH_ACTION.DEC) {
                        curr_update_value++;
                        // the current batch size is decreased
                        curr_batch_size -= Common.mapInttoPower.get(curr_update_value);
                        update_bacth_size_all(curr_batch_size);
                    } else // either none or inc action
                    {
                        curr_update_value = 0;
                        // the current batch size is decreased
                        curr_batch_size -= Common.mapInttoPower.get(curr_update_value);
                        update_bacth_size_all(curr_batch_size);
                    }
                    prev_batch_action = BATCH_ACTION.DEC;
                }
            } else if ( Math.abs(data_rate - acked_rate) <= diff_threshold_dec )
            {
                prev_batch_action = BATCH_ACTION.NONE;
            }
            m_oLogger.Info(" Iter:" + Long.toString(iter) +
                    "BATCH ACTION:" + mapBatchActionToString.get(prev_batch_action) +
                    "UPDATE VALUE:" + Long.toString(curr_update_value) +
                    "BATCH SIZE:" + Long.toString(curr_batch_size)
            );
            try {
                Thread.sleep(m_nLBPeriod);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

}
