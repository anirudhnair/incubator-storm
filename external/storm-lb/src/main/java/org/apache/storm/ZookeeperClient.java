package org.apache.storm;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
/**
 * Created by anirudhnair on 5/20/16.
 */
public class ZookeeperClient {

    private String      m_sHost;
    private ZooKeeper   m_oZooKeeper;
    private java.util.concurrent.CountDownLatch m_oConnectedSignal;
    Logger  m_oLogger;
    public ZookeeperClient(String host, Logger logger)
    {
        m_sHost = host;
        m_oConnectedSignal = new java.util.concurrent.CountDownLatch(1);
        m_oLogger = logger;
    }

    public int Connect()
    {
//host - local host
        //5000 - number of milliseconds to try to connect to zookeeper
        try {
            m_oZooKeeper = new ZooKeeper(m_sHost, 5000, new Watcher() {
                public void process(WatchedEvent event) {
                    if(event.getState() == KeeperState.SyncConnected)
                    {
                        m_oConnectedSignal.countDown();
                    }
                }
            });


            m_oConnectedSignal.await();
        } catch (Exception e)
        {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;
        }
        return Common.SUCCESS;
    }

    public int SetData(String path, byte []data)
    {
        try {
            m_oZooKeeper.setData(path, data, -1);
        } catch (KeeperException e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;
        } catch (InterruptedException e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return Common.FAILURE;
        }
        m_oLogger.Info("Added data to path " + path + " successfully.");
        return Common.SUCCESS;
    }

    public byte[] GetData(String path)
    {
        byte [] data = null;

        try {
            data = m_oZooKeeper.getData(path, true, m_oZooKeeper.exists(path, true));
        } catch (KeeperException e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return null;        }
        catch (InterruptedException e) {
            m_oLogger.Error(m_oLogger.StackTraceToString(e));
            return null;
        }
        m_oLogger.Info("Got data from path " + path + " successfully.");
        return data;
    }
}
