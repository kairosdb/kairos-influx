package org.kairosdb.telegraf;

import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;

import static com.google.common.base.Preconditions.checkState;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;
import static com.google.common.base.Preconditions.checkNotNull;


public class Metric
{
    private String name;
    private ImmutableSortedMap<String, String> tags;
    private DataPoint dataPoint;

    public Metric(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint)
    {
        name = checkNotNullOrEmpty(metricName, "metricName must not be null or empty");
        checkState(tags.size() > 0, "You must have at least one tag");
        this.tags = checkNotNull(tags, "tags must not be null");
        this.dataPoint = checkNotNull(dataPoint, "dataPoint must not be null");
    }

    public String getName()
    {
        return name;
    }

    public ImmutableSortedMap<String, String> getTags()
    {
        return tags;
    }

    public DataPoint getDataPoint()
    {
        return dataPoint;
    }
}
