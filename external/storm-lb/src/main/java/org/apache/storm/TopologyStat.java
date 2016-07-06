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

    // input data rate
    private LinkedList<FieldValue<Long>>    tupleDataRateCountList;
    private long                            totalIncomingTupleCount;

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
        tupleDataRateCountList  = new LinkedList<>();
        totalIncomingTupleCount = 0;
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

    public void UpdateEmitCount(long total_emit)
    {
        m_oLockW.lock();
        long new_count = total_emit - totalIncomingTupleCount;
        totalIncomingTupleCount+=new_count;
        tupleDataRateCountList.addLast(new FieldValue<Long>(System.currentTimeMillis(),new_count));
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

    // acks per second
    public double GetAvgAckCount(int readings)
    {
        m_oLockR.lock();
        double sum = 0.0;
        int count = 0;
        long last_time = 0, first_time = 0;
        Iterator<FieldValue<Long>> itr = tupleAckedCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> tp = itr.next();
            sum+=tp.value_;
            if (count == 1) last_time = tp.time_;
            else if(count == readings) {
                first_time = tp.time_;
                break;
            }
        }
        m_oLockR.unlock();
        if (count > 0)
            return ((double)sum/(last_time-first_time))*1000;
        else return sum;
    }

    public FieldValue<Long> GetAckedCountList()
    {
        m_oLockR.lock();
        Object obj = tupleAckedCountList.clone();
        m_oLockR.unlock();
        return (FieldValue<Long>)obj;
    }

    public double GetAvgEmitCount(int readings)
    {
        m_oLockR.lock();
        double sum = 0.0;
        int count = 0;
        long last_time = 0, first_time = 0;
        Iterator<FieldValue<Long>> itr = tupleDataRateCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> tp = itr.next();
            sum+=tp.value_;
            if (count == 1) last_time = tp.time_;
            else if(count == readings) {
                first_time = tp.time_;
                break;
            }
        }
        m_oLockR.unlock();
        if (count > 0)
            return ((double)sum/(last_time-first_time))*1000;
        else return sum;
    }

    public FieldValue<Long> GetEmitCountList()
    {
        m_oLockR.lock();
        Object obj = tupleDataRateCountList.clone();
        m_oLockR.unlock();
        return (FieldValue<Long>)obj;
    }

    public double GetAvgLatecny_ns(int readings)
    {
        m_oLockR.lock();
        double sum = 0;
        int count = 0;
        Iterator<FieldValue<Long>> itr = avgLatencyList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> lat = itr.next();
            sum+=lat.value_;
            if(count == readings) break;
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

    public double GetAvgFailedCount(int readings)
    {
        m_oLockR.lock();
        double sum = 0.0;
        int count = 0;
        long last_time = 0, first_time = 0;
        Iterator<FieldValue<Long>> itr = tupleFailedCountList.descendingIterator();
        while(itr.hasNext())
        {
            count++;
            FieldValue<Long> ft = itr.next();
            sum+=ft.value_;
            if (count == 1) last_time = ft.time_;
            else if(count == readings) {
                first_time = ft.time_;
                break;
            }
        }
        m_oLockR.unlock();
        if (count > 0)
            return ((double)sum/(last_time-first_time))*1000;
        else return sum;
    }

    public String PrintStatus()
    {
        String status =
                " Throughput 10 readings " + Double.toString(GetAvgAckCount(10)) + "\n" +
                " Throughput 20 readings " +  Double.toString(GetAvgAckCount(20))   + "\n" +
                " Throughput 100 readings " +  Double.toString(GetAvgAckCount(100))   + "\n" +
                " Data Rate 10 readings " +  Double.toString(GetAvgEmitCount(10))   + "\n" +
                " Data Rate 20 readings " +  Double.toString(GetAvgEmitCount(20))   + "\n" +
                " Data Rate 100 readings " + Double.toString(GetAvgEmitCount(100))    + "\n" +
                " Latency Mean " +   Double.toString(hist_latecny.getMean())  + "\n" +
                " Latency 10 readings " +  Double.toString(GetAvgLatecny_ns(10))   + "\n" +
                " Latency 20 readings " +  Double.toString(GetAvgLatecny_ns(20))   + "\n" +
                " Latency 100 readings " +  Double.toString(GetAvgLatecny_ns(100))   + "\n" +
                " Latency 99 " + Double.toString(hist_latecny.getValueAtPercentile(99.0))  + "\n" +
                " Latency 99.9 " + Double.toString(hist_latecny.getValueAtPercentile(99.9))  + "\n" +
                " Failed 10 readings " +  Double.toString(GetAvgFailedCount(10))   + "\n" +
                " Failed 20 readings " +   Double.toString(GetAvgFailedCount(20))  + "\n" +
                " Failed 100 readings " +  Double.toString(GetAvgFailedCount(100)) + "\n";

        return status;


    }

}
