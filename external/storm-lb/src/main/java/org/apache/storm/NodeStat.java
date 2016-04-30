package org.apache.storm;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by anirudhnair on 3/4/16.
 */
public class NodeStat {

    // three stats 1. No of messages in and out 2. memory usage 3. cpu usage

    // no of messages



    private LinkedList<FieldValue<Long>>    messageCountList;
    private long                               totalMessageCount;

    // memory
    private LinkedList<FieldValue<Float>>      memoryUsageList;

    //cpu
    private LinkedList<FieldValue<Float>>      cpuUsageCountList;

    //power
    private LinkedList<FieldValue<Float>>      powerUsageCountList;

    private ReentrantReadWriteLock        m_oReadWriteLock;
    private Lock                          m_oLockR;
    private Lock 						  m_oLockW;


    public NodeStat() {
        messageCountList  = new LinkedList<>();
        memoryUsageList   = new LinkedList<>();
        cpuUsageCountList = new LinkedList<>();
        powerUsageCountList = new LinkedList<>();
        m_oReadWriteLock  = new ReentrantReadWriteLock();
        m_oLockR          = m_oReadWriteLock.readLock();
        m_oLockW          = m_oReadWriteLock.writeLock();
        totalMessageCount = 0;
    }

    public void AddMessageCount(long count)
    {
        m_oLockW.lock();
        messageCountList.addLast(new FieldValue<Long>(System.currentTimeMillis(), count));
        m_oLockW.unlock();
        totalMessageCount+=count;
    }

    public void AddMemUsgae(float mem)
    {
        m_oLockW.lock();
        memoryUsageList.addLast(new FieldValue<Float>(System.currentTimeMillis(), mem));
        m_oLockW.unlock();
    }

    public void AddCPUUsage(float cpu)
    {
        m_oLockW.lock();
        memoryUsageList.addLast(new FieldValue<Float>(System.currentTimeMillis(), cpu));
        m_oLockW.unlock();

    }

    public void AddPowerUsage( float power)
    {
        m_oLockW.lock();
        powerUsageCountList.addLast(new FieldValue<Float>(System.currentTimeMillis(), power));
        m_oLockW.unlock();
    }

    public long GetMessageCount(long millSecRange)
    {
        m_oLockR.lock();
        long sum = 0;
        long totalTime = 0;
        long currTime = System.currentTimeMillis();
        Iterator<FieldValue<Long>> itr = messageCountList.descendingIterator();
        while(itr.hasNext())
        {
            FieldValue<Long> msgC = itr.next();
            sum+=msgC.value_;
            totalTime+=(currTime - msgC.time_);
            if(totalTime > millSecRange) break;
        }
        m_oLockR.unlock();
        return sum;
    }

    public FieldValue<Long> GetMessageCountList()
    {
        return (FieldValue<Long>)messageCountList.clone();
    }

    public float GetAvgMemUsage(long millSecRange) {
        m_oLockR.lock();
        float sum = 0f;
        int count = 0;
        long totalTime = 0;
        long currTime = System.currentTimeMillis();
        Iterator<FieldValue<Float>> itr = memoryUsageList.descendingIterator();
        while (itr.hasNext()) {
            count++;
            FieldValue<Float> memU = itr.next();
            sum += memU.value_;
            totalTime += (currTime - memU.time_);
            if (totalTime > millSecRange) break;
        }
        m_oLockR.unlock();

        if(count > 0)
            return sum/(float)count;
        else return sum;

    }

    public FieldValue<Float> GetMemUsgaeList()
    {
        return (FieldValue<Float>)memoryUsageList.clone();
    }

    public float GetAvgCPUUsage(long millSecRange)
    {
        m_oLockR.lock();
        float sum = 0f;
        int count = 0;
        long totalTime = 0;
        long currTime = System.currentTimeMillis();
        Iterator<FieldValue<Float>> itr = cpuUsageCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Float> cpuU = itr.next();
            sum+=cpuU.value_;
            totalTime+=(currTime - cpuU.time_);
            if(totalTime > millSecRange) break;
        }
        m_oLockR.unlock();

        if(count > 0)
            return sum/(float)count;
        else return sum;
    }

    public FieldValue<Float> GetCPUUsgaeList()
    {
        return (FieldValue<Float>)cpuUsageCountList.clone();
    }


    public float GetAvgPowerUsage(long millSecRange)
    {
        m_oLockR.lock();
        float sum = 0f;
        int count = 0;
        long totalTime = 0;
        long currTime = System.currentTimeMillis();
        Iterator<FieldValue<Float>> itr = powerUsageCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Float> powerU = itr.next();
            sum+=powerU.value_;
            totalTime+=(currTime - powerU.time_);
            if(totalTime > millSecRange) break;
        }
        m_oLockR.unlock();

        if(count > 0)
            return sum/(float)count;
        else return sum;
    }

    public FieldValue<Float> GetPowerUsgaeList()
    {
        return (FieldValue<Float>)powerUsageCountList.clone();
    }
}
