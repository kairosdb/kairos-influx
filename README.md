# kairos-influx

This KairosDB plugin takes metrics sent from [Telegraf](https://docs.influxdata.com/telegraf/) 
in the [InfluxDB Line Protocol format](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/#syntax) 
and writes them to KairosDB.
The plugin will accept gzipped and non-gzipped data from Telegraf or any other application that writes data to InfluxDB.

###Influx URL
The plugin accepts version 1 and 2 of the influx api.  You will set the url to 
```
http://kairos-server:8080/api/influx
```
Replace kairos-server with your kairos instance domain.

###Bucket or DB
Depending on what version of the api you tell your client to you Kairos can prepend
the bucket or db to the metric before storing it.  Otherwise these parameters are ignored

###Precision
Kairos will honor the precision sent with each api call.

###Other influx parameters
All other influx parameters are ignored by the plugin at this time.

# Configuration
Here is a sample configuration for this plugin. 


```
kairosdb: {
	service.influx: "org.kairosdb.influxdb.InfluxModule"
	service_folder.influx=lib/telegraf

	influx: {
		prefix: "telegraph"
		metric_separator: "."
		include_bucket_or_db: true
		dropMetrics: ["^swap.used.*$", "^kernel.interrupts$"]
		dropTags: ["^usage_irq$", "^usage_idle$"]
	}
}
```

These optional properties provide ways to manipulate or restrict the data written to KairosDB.


| Property                       | Description                                                             |
|--------------------------------|-------------------------------------------------------------------------|
| kairosdb.influx.prefix      | Prefix prepended to each metric name. |
| kairosdb.influx.metric_separator | String to use to separate portions of a metric name when adding the prefix or bucket names.  Defaults to '.' |
| kariosdb.influx.include_bucket_or_db | This prepends the bucket or db name (depending on which influx api version you are useing) to the metric name.  This goes after the prefix. (true/false) |
| kairosdb.influx.dropMetrics | This is a list of regular expressions. Metric names that match any of the regular expressions are ignored and not added to KairosDB. | 
| kairosdb.influx.dropTags   | This is a list of regular expressions. Tag names that match any of the expressions are not included in metrics written to KairosDB. |


The optional prefix property adds the prefix string to the beginning of each metric name. 

# Internal Metrics
The plugin writes these metrics to KairosDB:

| Metric Name | Tags | Description |
| ----------- | ---- | ----------- |
| kairosdb.influx.ingest_count | status, host | This is the number of metrics ingested from Telegraf. Status is either "success" or "failed". Host is the name of the KairosDB host that reported the metric. |
| kairosdb.influx.exception_count | exception, host | This is a count of exceptions when ingesting. The exception tag is the exception name. Host is the name of the KairosDB host that reported the metric. | 
| kairosdb.influx.metrics-dropped.count | host | This is a count of the number of metrics (measurement + field name) dropped (ignored). Host is the name of the KairosDB host that reported the metric. |
| kairosdb.influx.tags-dropped.count | host | This is a count of the number of tags dropped (ignored). Host is the name of the KairosDB host that reported the metric.|