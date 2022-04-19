package org.kairosdb.influxdb;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.Preconditions;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkNotNull;

public class MetricWriter
{
    private final Publisher<DataPointEvent> dataPointPublisher;

    @Inject
    public MetricWriter(FilterEventBus eventBus)
    {
        checkNotNull(eventBus, "eventBus must not be null");
        dataPointPublisher = eventBus.createPublisher(DataPointEvent.class);
    }

    /*public void write(String metricName, DataPoint datapoint)
    {
        write(metricName, ImmutableSortedMap.of(), datapoint);
    }*/

    public void write(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint)
    {
        dataPointPublisher.post(new DataPointEvent(metricName, tags, dataPoint));
    }
}
