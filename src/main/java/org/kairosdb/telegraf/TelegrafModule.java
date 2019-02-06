package org.kairosdb.telegraf;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class TelegrafModule extends AbstractModule
{
    protected void configure()
    {
        bind(TelegrafResource.class).in(Singleton.class);
        bind(InfluxParser.class).in(Singleton.class);
        bind(MetricWriter.class).in(Singleton.class);
    }
}
