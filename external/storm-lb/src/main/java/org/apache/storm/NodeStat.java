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
        long diff = count - totalMessageCount;
        m_oLockW.lock();
        messageCountList.addLast(new FieldValue<Long>(System.currentTimeMillis(), diff));
        m_oLockW.unlock();
        totalMessageCount+=diff;
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
        cpuUsageCountList.addLast(new FieldValue<Float>(System.currentTimeMillis(), cpu));
        m_oLockW.unlock();

    }

    public void AddPowerUsage( float power)
    {
        m_oLockW.lock();
        powerUsageCountList.addLast(new FieldValue<Float>(System.currentTimeMillis(), power));
        m_oLockW.unlock();
    }

    //return message count / sec
    public double GetAvgMessageCount(int readings)
    {
        m_oLockR.lock();
        long sum = 0;
        long totalTime = 0;
        long last_time = 0, first_time = 0;
        int count = 0;
        Iterator<FieldValue<Long>> itr = messageCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> msgC = itr.next();
            sum+=msgC.value_;
            if (count == 1) last_time = msgC.time_;
            else if(count == readings) {
                first_time = msgC.time_;
                break;
            }
        }
        m_oLockR.unlock();
        if (count > 0)
            return ((double)sum/(last_time-first_time))*1000;
        else return sum; //0
    }

    public FieldValue<Long> GetMessageCountList()
    {
        return (FieldValue<Long>)messageCountList.clone();
    }

    public double GetAvgMemUsage(int readings) {
        m_oLockR.lock();
        double sum = 0.0;
        int count = 0;
        Iterator<FieldValue<Float>> itr = memoryUsageList.descendingIterator();
        while (itr.hasNext()) {
            count++;
            FieldValue<Float> memU = itr.next();
            sum += memU.value_;
            if (count == readings) break;
        }
        m_oLockR.unlock();

        if(count > 0)
            return sum/count;
        else return sum;

    }

    public FieldValue<Float> GetMemUsgaeList()
    {
        return (FieldValue<Float>)memoryUsageList.clone();
    }

    public double GetAvgCPUUsage(int readings)
    {
        m_oLockR.lock();
        double sum = 0.0;
        int count = 0;
        Iterator<FieldValue<Float>> itr = cpuUsageCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Float> cpuU = itr.next();
            sum+=cpuU.value_;
            if(count == readings) break;
        }
        m_oLockR.unlock();

        if(count > 0)
            return sum/count;
        else return sum;
    }

    public FieldValue<Float> GetCPUUsgaeList()
    {
        return (FieldValue<Float>)cpuUsageCountList.clone();
    }


    public double GetAvgPowerUsage(int readings)
    {
        m_oLockR.lock();
        double sum = 0.0;
        int count = 0;
        Iterator<FieldValue<Float>> itr = powerUsageCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Float> powerU = itr.next();
            sum+=powerU.value_;
            if(count == readings) break;
        }
        m_oLockR.unlock();

        if(count > 0)
            return sum/count;
        else return sum;
    }

    public double GetEnergyUsage(int readings)
    {
        m_oLockR.lock();
        double sum = 0.0;
        int count = 0;
        long last_time = 0, first_time = 0;
        Iterator<FieldValue<Float>> itr = powerUsageCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Float> powerU = itr.next();
            sum+=powerU.value_;
            if (count == 1) last_time = powerU.time_;
            else if(count == readings) {
                first_time = powerU.time_;
                break;
            }
        }
        m_oLockR.unlock();
        if(count > 0) {
            double avg_power = sum/count;
            return avg_power * (last_time - first_time);
        }
        else return sum;

    }

    public FieldValue<Float> GetPowerUsgaeList()
    {
        return (FieldValue<Float>)powerUsageCountList.clone();
    }

    public String PrintStatus()
    {
        String status =
                " MessageCount_10 " + Double.toString(GetAvgMessageCount(10)) + " " +
                        " CPUUtil_10 " +  Double.toString(GetAvgCPUUsage(10))   + " " +
                        " MemUsage_10 " +  Double.toString(GetAvgMemUsage(10))   + " " +
                        " Power_10 " +  Double.toString(GetAvgPowerUsage(10));
        return status;


    }
}
