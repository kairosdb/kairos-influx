package org.kairosdb.telegraf;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class MetricWriter
{
    private final Publisher<DataPointEvent> dataPointPublisher;
    private final String host;

    @Inject
    public MetricWriter(FilterEventBus eventBus) throws UnknownHostException
    {
        this(eventBus, InetAddress.getLocalHost().getHostName());
    }

    public MetricWriter(FilterEventBus eventBus, String host)
    {
        checkNotNull(eventBus, "eventBus must not be null");
        dataPointPublisher = eventBus.createPublisher(DataPointEvent.class);
        this.host = checkNotNullOrEmpty(host, "host must not be null or empty");
    }

    public void write(String metricName, DataPoint datapoint)
    {
        write(metricName, ImmutableSortedMap.of(), datapoint);
    }

    public void write(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint)
    {
        ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
        builder.putAll(tags);
        if (!tags.containsKey("host"))
        {
            builder.put("host", host);
        }

        dataPointPublisher.post(new DataPointEvent(metricName, builder.build(), dataPoint));
    }
}
