package org.apache.storm;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by anirudhnair on 3/7/16.
 */
public class NodeStatClient {

    private NodeStatServer.Client m_oClient;
    private Logger m_oLogger;
    private TTransport transport;

    public NodeStatClient()
    {

    }

    public int Init(String hostIP, Logger oLogger)
    {
        m_oLogger	= oLogger;
        transport = new TFramedTransport(new TSocket(hostIP, Common.STAT_PORT));
        try {
            transport.open();
        } catch (TTransportException e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            m_oLogger.Error("Failed to connect to " + hostIP + " for stats collection");
            return Common.FAILURE;
        }
        m_oClient = new NodeStatServer.Client(new TBinaryProtocol(transport));
        m_oLogger.Info("Connect to " + hostIP + " for stats collection");
        return Common.SUCCESS;
    }

    public double CPUUsage() throws TException
    {
        return m_oClient.GetCPUUsage();
    }

    public double MemUsage() throws TException
    {
        return m_oClient.GetMemUsage();
    }

    // more power related stats here
    public double PowerUsgae() throws TException
    {
        return m_oClient.GetPowerUsage();
    }
}
