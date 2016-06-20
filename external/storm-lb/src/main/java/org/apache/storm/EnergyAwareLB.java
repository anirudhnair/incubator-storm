package org.apache.storm;

import backtype.storm.generated.Nimbus;

/**
 * Created by anirudhnair on 6/17/16.
 */
public class EnergyAwareLB extends AbstLoadBalance {
    @Override
    public void load_balance() {

    }

    @Override
    public int Init(StatsCollector oCollect, String sTopoName, ZookeeperClient zk_client, LBConfigReader oConfig, Logger oLogger, Nimbus.Client nimbus_client) {
        return 0;
    }
}
