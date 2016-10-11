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



    }
}
