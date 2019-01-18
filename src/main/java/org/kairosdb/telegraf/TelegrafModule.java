package org.kairosdb.telegraf;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class TelegrafModule extends AbstractModule
{
    protected void configure()
    {
//        bind(GzipMessageBodyReader.class).in(Singleton.class);
        bind(TelegrafResource.class).in(Singleton.class);
    }
}
