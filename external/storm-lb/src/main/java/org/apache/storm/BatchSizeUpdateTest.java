package org.apache.storm;

import backtype.storm.generated.Nimbus;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift.TException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;

/**
 * Created by anirudhnair on 6/29/16.
 */
public class BatchSizeUpdateTest {

    ;
    public static int update_bacth_size_all(int batch_size,LBTopologyInfo   m_oTopoInfo)
    {
        ArrayList<LBExecutorInfo> all_execs = new ArrayList<LBExecutorInfo>();
        all_execs.addAll(m_oTopoInfo.GetSpoutInfo().m_lExecs);
        for(LBComponentInfo info: m_oTopoInfo.GetBolts().values())
            all_execs.addAll(info.m_lExecs);
        for(LBExecutorInfo info: all_execs)
        {
            if( Common.FAILURE == info.setBatchSize(batch_size))
            {
                System.out.println("Failed to set batch_size on " + Integer.toString(info.getID()));
            }
        }
        return Common.SUCCESS;
    }

    public static void main(String[] args) throws InterruptedException {

        Logger logger = new Logger();
        logger.Initialize("test_log");
        ZookeeperClient zk = new ZookeeperClient("localhost",logger);
        zk.Connect();
        Map clusterConf = Utils.readStormConfig();
        Nimbus.Client oNimbus = NimbusClient.getConfiguredClient(clusterConf).getClient();
        LBTopologyInfo   m_oTopoInfo = new LBTopologyInfo("word",logger,zk);
        m_oTopoInfo.Init(oNimbus);
        int batch_size = 10;
        int size  = 0;
        while(true){
            batch_size = Math.max(10,(size + 10)%60);
            System.out.println(batch_size);
            update_bacth_size_all(batch_size,m_oTopoInfo);
            size+=10;
            Thread.sleep(30000);
        }

    }
}
