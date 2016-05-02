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

    public static enum LOAD_BALANCERS {SIMPLE, SLA_AWARE, ENERGY_AWARE, NONE};
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




}
