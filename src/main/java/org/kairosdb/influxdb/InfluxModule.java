package org.kairosdb.influxdb;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class InfluxModule extends AbstractModule
{
    protected void configure()
    {
        bind(InfluxResource.class).in(Singleton.class);
        bind(InfluxParser.class).in(Singleton.class);
        bind(MetricWriter.class).in(Singleton.class);
    }
}
