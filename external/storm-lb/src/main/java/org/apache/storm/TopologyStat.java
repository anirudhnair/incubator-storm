package org.apache.storm;
import org.HdrHistogram.Histogram;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by anirudhnair on 3/3/16.
 * For the topology stat to work the storm topology should be
 * run with acking enabled
 */
public class TopologyStat {





    //throughput
    private LinkedList<FieldValue<Long>>    tupleAckedCountList;
    private long                            totalTupleAckedCount;

    //latency
    private LinkedList<FieldValue<Long>>    avgLatencyList;
    private Histogram                       hist_latecny;

    // failed tuples
    private LinkedList<FieldValue<Long>>    tupleFailedCountList;
    private long                            totalTupleFailedCount;

    private ReentrantReadWriteLock    m_oReadWriteLock;
    private Lock                      m_oLockR;
    private Lock 					  m_oLockW;

    public TopologyStat()
    {
        tupleAckedCountList     = new LinkedList<>();
        totalTupleAckedCount    = 0;
        avgLatencyList          = new LinkedList<>();
        hist_latecny            = new Histogram(3600000000000L, 3);
        tupleFailedCountList    = new LinkedList<>();
        totalTupleFailedCount   = 0;
        m_oReadWriteLock        = new ReentrantReadWriteLock();
        m_oLockR                = m_oReadWriteLock.readLock();
        m_oLockW                = m_oReadWriteLock.writeLock();
    }

    public void UpdateAckedCount(long total_acked)
    {
        m_oLockW.lock();
        long new_count = total_acked - totalTupleAckedCount;
        totalTupleAckedCount+=new_count;
        tupleAckedCountList.addLast(new FieldValue<Long>(System.currentTimeMillis(),new_count));
        m_oLockW.unlock();
    }

    public void AddLatency(Histogram hist_)
    {
        if(hist_ != null)
        {
            m_oLockW.lock();
            long avg = (long)hist_.getMean();
            avgLatencyList.addLast(new FieldValue<Long>(System.currentTimeMillis(),avg));
            hist_latecny.add(hist_);
            m_oLockW.unlock();
        }
    }

    public void UpdateFailedCount(long total_failed)
    {
        m_oLockW.lock();
        long new_count = total_failed - totalTupleFailedCount;
        totalTupleFailedCount+=new_count;
        tupleFailedCountList.addLast(new FieldValue<Long>(System.currentTimeMillis(),new_count));
        m_oLockW.unlock();
    }

    public long GetAvgAckCount(long milliSecRange)
    {
        m_oLockR.lock();
        long sum = 0;
        int count = 0;
        long totalTime = 0;
        long currTime = System.currentTimeMillis();
        Iterator<FieldValue<Long>> itr = tupleAckedCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> tp = itr.next();
            sum+=tp.value_;
            totalTime+=(currTime - tp.time_);
            if(totalTime > milliSecRange) break;
        }
        m_oLockR.unlock();
        if(count > 0)
            return sum/count;
        else return sum;
    }

    public FieldValue<Long> GetAckedCountList()
    {
        m_oLockR.lock();
        Object obj = tupleAckedCountList.clone();
        m_oLockR.unlock();
        return (FieldValue<Long>)obj;
    }

    public long GetAvgLatecny_ns(long milliSecRange)
    {
        m_oLockR.lock();
        long sum = 0;
        int count = 0;
        long totalTime = 0;
        long currTime = System.currentTimeMillis();
        Iterator<FieldValue<Long>> itr = avgLatencyList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> lat = itr.next();
            sum+=lat.value_;
            totalTime+=(currTime - lat.time_);
            if(totalTime > milliSecRange) break;
        }
        m_oLockR.unlock();
        if(count > 0)
            return sum/count;
        else return sum;
    }

    public FieldValue<Long> GetLatencyList()
    {
        m_oLockR.lock();
        Object obj = avgLatencyList.clone();
        m_oLockR.unlock();
        return (FieldValue<Long>)obj;
    }

    public Histogram GetLatencyHist()
    {
        return hist_latecny.copy();
    }

    public FieldValue<Long> GetFailedCountList()
    {
        m_oLockR.lock();
        Object obj = tupleFailedCountList.clone();
        m_oLockR.unlock();
        return (FieldValue<Long>)obj;
    }

    public long GetAvgFailedCount(long milliSecRange)
    {
        m_oLockR.lock();
        long sum = 0;
        int count = 0;
        long totalTime = 0;
        long currTime = System.currentTimeMillis();
        Iterator<FieldValue<Long>> itr = tupleFailedCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> ft = itr.next();
            sum+=ft.value_;
            totalTime+=(currTime - ft.time_);
            if(totalTime > milliSecRange) break;
        }
        m_oLockR.unlock();
        if(count > 0)
            return sum/count;
        else return sum;
    }

}
