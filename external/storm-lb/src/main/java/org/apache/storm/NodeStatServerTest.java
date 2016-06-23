package org.apache.storm;

import org.apache.storm.Logger;
import org.apache.storm.NodeStatClient;
import org.apache.thrift.TException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static java.lang.System.exit;

/**
 * Created by anirudhnair on 6/23/16.
 */
public class NodeStatServerTest {

    public static void main(String[] args) throws InterruptedException, TException {
        String sIP = args[0];
        Logger logger = new Logger();
        String timestamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
        logger.Initialize("/home/ajayaku2/log/NodeStatTest_" + timestamp + ".log");
        NodeStatClient client = new NodeStatClient();
        client.Init(sIP, logger);
        while(true)
        {
            System.out.println(client.CPUUsage());
            System.out.println(client.MemUsage());
            System.out.println(client.PowerUsgae());
            System.out.println("-----------------");
            Thread.sleep(10000);
        }

    }
}
