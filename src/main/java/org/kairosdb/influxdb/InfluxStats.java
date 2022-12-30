package org.kairosdb.influxdb;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface InfluxStats
{
	LongCollector metricsDropped();
	LongCollector tagsDropped();
	LongCollector exception(@Key("exception")String exception);
	LongCollector ingest(@Key("status")String status);
}
