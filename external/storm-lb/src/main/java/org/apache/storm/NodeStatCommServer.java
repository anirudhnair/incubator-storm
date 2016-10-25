package org.apache.storm;

import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by anirudhnair on 3/7/16.
 */
public class NodeStatCommServer {

    private Thread 					m_oStatServThread;
    private NodeStatServerImpl      m_oStatImpl;
    private Logger                  m_oLogger;

    public NodeStatCommServer()
    {

    }

    public int Init(String sIP,Logger oLogger)
    {
        m_oLogger = oLogger;
        m_oStatImpl = new NodeStatServerImpl(sIP);
        m_oStatServThread = new Thread(new Runnable() {
            public void run() {
                try {
                    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(Common.STAT_PORT);
                    TNonblockingServer.Args args = new TNonblockingServer.Args(serverTransport).processor(new NodeStatServer.Processor(m_oStatImpl));

                    args.transportFactory(new TFramedTransport.Factory(104857600));
                    TServer server = new TNonblockingServer(args);

                    server.serve();
                } catch (TException e)
                {
                    m_oLogger.Error(m_oLogger.StackTraceToString(e));

                }
                return;
            }
        });

        return Common.SUCCESS;
    }
    public void Start()
    {
        m_oStatServThread.start();
    }

    public void Wait()
    {
        try {
            m_oStatServThread.join();
        } catch (InterruptedException e)
        {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            System.exit(Common.FAILURE);
        }
    }

    public static void main( String[] args )
    {
        String sIP = null;
        try {
            sIP  = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e1) {
            System.out.print("Unknown Host Exception");
        }
        // start the service and wait for it
        NodeStatCommServer server = new NodeStatCommServer();
        Logger logger = new Logger();
        String timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
        logger.Initialize("/home/ajayaku2/log/statserver_" + sIP + ".log");
        server.Init(sIP,logger);
        server.Start();
        server.Wait();
        System.exit(Common.SUCCESS);
    }
}
