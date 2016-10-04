package org.apache.storm;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

import java.io.IOException;

/**
 * Created by anirudhnair on 6/21/16.
 */
public interface ILBTopoIface {
    public StormTopology getTopology(Config config, String topoConf, String lbConf, String dataRates) throws
            IOException;
}
