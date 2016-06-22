package org.apache.storm;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by anirudhnair on 3/4/16.
 */
public final class Common {
    public static final int SUCCESS = 0;
    public static final int FAILURE = 1;
    public static final int DATA_RATE_CHANGE_INTERVAL = 10000;//10 secs
    public static enum LOAD_BALANCER {DATA_RATE_AWARE, LOAD_AWARE, ENERGY_AWARE, NONE};
    public static enum SLA_TYPE {LATENCY_MEAN, LATENCY_99, LATENCY_999};
    public static final Map<LOAD_BALANCER, String> mapLBTypeToName = Collections.unmodifiableMap(
            new HashMap<LOAD_BALANCER, String> () {{
                put(LOAD_BALANCER.DATA_RATE_AWARE,"DATA_RATE_AWARE");
                put(LOAD_BALANCER.LOAD_AWARE,"LOAD_AWARE");
                put(LOAD_BALANCER.ENERGY_AWARE,"ENERGY_AWARE");
                put(LOAD_BALANCER.NONE,"NONE");
            }});

    public static final Map<String, SLA_TYPE> mapSLAStrToType = Collections.unmodifiableMap(
            new HashMap<String, SLA_TYPE> () {{
                put("mean",SLA_TYPE.LATENCY_MEAN);
                put("99",SLA_TYPE.LATENCY_99);
                put("999",SLA_TYPE.LATENCY_999);
            }});

    public static final Map<SLA_TYPE,String > mapSLATypeToStr = Collections.unmodifiableMap(
            new HashMap<SLA_TYPE,String> () {{
                put(SLA_TYPE.LATENCY_MEAN,"mean");
                put(SLA_TYPE.LATENCY_99,"99");
                put(SLA_TYPE.LATENCY_999,"999");
            }});

    public static final int STAT_PORT= 9567;
    public static final long STAT_COLLECTION_INTERVAL = 10000; // 10s
    public static final String POWER_SERVER = "tarek4234-ckt23-pdu.cs.illinois.edu";
    public static final Map<String, Long> mapNodetoPort = Collections.unmodifiableMap(
        new HashMap<String, Long> () {{
            put("172.22.68.60",24L); //tarekc41
            put("172.22.68.61",23L); // nimbus
            put("172.22.68.62",22L);
            put("172.22.68.63",21L);
            put("172.22.68.64",20L);
            put("172.22.68.65",19L); // login warning
            put("172.22.68.66",18L);
            put("172.22.68.67",16L);
            put("172.22.68.68",15L);
            put("172.22.68.69",14L);
            put("172.22.68.70",13L);
            put("172.22.68.71",12L);
            put("172.22.68.72",11L);
            put("172.22.68.73",10L);
            put("172.22.68.74",8L); // login warning
            put("172.22.68.75",7L);
            put("172.22.68.76",6L);
            put("172.22.68.77",5L);
            put("172.22.68.78",4L); // node down
            put("172.22.68.79",3L); // tarekc60
        }});

    public static final String NIMBUS = "172.22.68.61";

    public static final String DYNAMIC_BATCHING_ROOT = "dynamic-batching";

    public static String GeneratePowerCommand(String sNodeIP)
    {
        // check if node is in the map
        if(mapNodetoPort.containsKey(sNodeIP))
        {
            // get the port id
            long portId = mapNodetoPort.get(sNodeIP);

            //generate the command
            String cmd = "/home/ajayaku2/avocent_measure.php " + POWER_SERVER + " " + Long.toString(portId);

            return cmd;
        }
        else {
            return "NULL";
        }

    }

    /*  (str storm-id "-" comp-id "-" (first executor-id) "-size")
   (str storm-id "-" comp-id "-" (first executor-id) "-interval")
   */
    public static String DynamicBatchZnodeSize(String topoID, String compID, int execID) {
        return topoID + "-" + compID + "-" + Integer.toString(execID) + "-size";
    }

    public static String DynamicBatchZnodeInterval(String topoID, String compID, int execID) {
        return topoID + "-" + compID + "-" + Integer.toString(execID) + "-interval";
    }

    public static final Map<Integer, Integer> mapInttoPower = Collections.unmodifiableMap(
            new HashMap<Integer, Integer> () {{
                put(0,1);
                put(1,2);
                put(2,4);
                put(3,8);
                put(4,16);
                put(5,32);
                put(6,64);
                put(7,128);
                put(8,256);
                put(9,512);
                put(10,1024);
                put(11,2048);
                }});


}
