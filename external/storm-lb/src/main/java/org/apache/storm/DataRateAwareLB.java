package org.apache.storm;

import backtype.storm.generated.Nimbus;

/**
 * Created by anirudhnair on 6/14/16.
 */
public class DataRateAwareLB extends AbstLoadBalance {

    @Override
    public int Init(StatsCollector oCollect, String sTopoName, ZookeeperClient zk_client, LBConfigReader
            oConfig, Logger oLogger,Nimbus.Client nimbus_client)
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
        while(true)
        {

            double acked_rate = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgAckCount(nStatReadings); // last 20 secs
            double data_rate = m_oStatsCollector.GetTopoStat(m_sTopoName).GetAvgEmitCount(nStatReadings);

            if( data_rate > acked_rate && (data_rate - acked_rate) > diff_threshold_inc && sla_satisfied())
            {
                if( prev_batch_action == BATCH_ACTION.INC)
                {
                    curr_update_value++;
                    curr_batch_size+= Common.mapInttoPower.get(curr_update_value);
                    update_bacth_size_all(curr_batch_size);
                } else // either none or dec action
                {
                    curr_update_value = 0;
                    curr_batch_size+= Common.mapInttoPower.get(curr_update_value);
                    update_bacth_size_all(curr_batch_size);
                }

                prev_batch_action = BATCH_ACTION.INC;

            } else if (Math.abs(data_rate - acked_rate) < diff_threshold_dec)
            {
                if( prev_batch_action == BATCH_ACTION.DEC)
                {
                    curr_update_value++;
                    curr_batch_size-= Common.mapInttoPower.get(curr_update_value);
                    update_bacth_size_all(curr_batch_size);
                } else // either none or inc action
                {
                    curr_update_value = 0;
                    curr_batch_size-= Common.mapInttoPower.get(curr_update_value);
                    update_bacth_size_all(curr_batch_size);
                }

                prev_batch_action = BATCH_ACTION.DEC;

            } else
            {
                prev_batch_action = BATCH_ACTION.NONE;
            }

            try {
                Thread.sleep(m_nLBPeriod);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

}
