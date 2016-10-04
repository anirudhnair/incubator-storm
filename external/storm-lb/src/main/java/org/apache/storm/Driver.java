package org.apache.storm;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;

import static backtype.storm.Config.STORM_ZOOKEEPER_PORT;
import static backtype.storm.Config.STORM_ZOOKEEPER_SERVERS;

/**
 * Created by anirudhnair on 6/20/16.
 */
public class Driver {

// responsible for the UI and starting the LB Client

    public static void main( String[] args ) throws Exception {
        Logger logger = new Logger();
        String timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
        String sZkHost = args[0];
        String filePath = args[1];
        String topoConf = args[2];
        String lbConf = args[3];
        String dataRates = args[4];
        logger.Initialize(filePath);

        LBClient client = new LBClient();
        client.Initialize(sZkHost,logger,topoConf,lbConf);
        Scanner m_oUserInput = new Scanner(System.in);
        while (true)
        {
            System.out.println("=========================");
            System.out.println("Enter choice");
            System.out.println("1. Submit Topology");
            System.out.println("2. Get Node Stat");
            System.out.println("3. Get TopoStat");
            System.out.println("Enter Input ");
            System.out.println("=========================");
            String sInput = m_oUserInput.nextLine();
            if( ! sInput.equals("1") && !sInput.equals("2") && !sInput.equals("3"))
            {
                System.out.println("Invalid input");
                continue;
            }

            if(sInput.equals("1"))
            {
                System.out.println("=========================");
                System.out.println("Enter Topology");
                System.out.println("1. RollingSort");
                System.out.println("2. SOL");
                System.out.println("3. WordCount");
                System.out.println("Enter Input ");
                System.out.println("=========================");
                sInput = m_oUserInput.nextLine();
                if( ! sInput.equals("1") && !sInput.equals("2") && !sInput.equals("3"))
                {
                    System.out.println("Invalid input");
                    continue;
                }

                System.out.println("=========================");
                System.out.println("Enter Load Balancing");
                System.out.println("1. Data Rate Aware");
                System.out.println("2. Load Aware");
                System.out.println("3. Energy");
                System.out.println("4. None");
                System.out.println("Enter Input ");
                System.out.println("=========================");
                String sLB = m_oUserInput.nextLine();
                if( ! sInput.equals("1") && !sInput.equals("2") && !sInput.equals("3")
                        && !sInput.equals("4"))
                {
                    System.out.println("Invalid input");
                    continue;
                }
                Common.LOAD_BALANCER lb = Common.LOAD_BALANCER.NONE;
                if(sLB.equals("1"))
                {
                    lb = Common.LOAD_BALANCER.DATA_RATE_AWARE;
                } else if(sLB.equals("2"))
                {
                    lb = Common.LOAD_BALANCER.LOAD_AWARE;
                } else if(sLB.equals("3"))
                {
                    lb = Common.LOAD_BALANCER.ENERGY_AWARE;
                } else if(sLB.equals("4"))
                {
                    lb = Common.LOAD_BALANCER.NONE;
                }


                Config conf = new Config();
                StormTopology topo = null;
                String topo_name = null;
                if(sInput.equals("1"))
                {
                    topo = new RollingSort().getTopology(conf,topoConf,lbConf,dataRates);
                    timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
                    topo_name = "RollingSort_" + timeStamp;

                } else if(sInput.equals("2")) {
                    topo = null; // todo
                } else if(sInput.equals("3")) {
                    topo = null; // todo
                }

                // topology is submitted

                client.SubmitTopology(topo_name,conf,topo,lb);
            } else if(sInput.equals("2"))
            {
                System.out.println("Enter Node IP");
                sInput = m_oUserInput.nextLine();
                System.out.println(client.GetNodeStat(sInput).PrintStatus());
            } else if(sInput.equals("3"))
            {
                System.out.println("Enter Topology Name");
                sInput = m_oUserInput.nextLine();
                System.out.println(client.GetTopologyStat(sInput).PrintStatus());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.print(e.toString());
            }
        }
    }


}
