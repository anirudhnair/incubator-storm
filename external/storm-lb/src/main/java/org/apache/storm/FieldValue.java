package org.apache.storm;

/**
 * Created by anirudhnair on 3/6/16.
 */
public class FieldValue<T> {
    public long  time_;
    public T value_;

    public FieldValue(long time, T value) {
        time_ = time;
        value_ = value;
    }
}
