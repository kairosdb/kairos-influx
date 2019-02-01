# telegraf-plugin

This KairosDB plugin takes metrics sent from [Telegraf](https://docs.influxdata.com/telegraf/) in the [InfluxDB Line Protocol format](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/#syntax) and writes them to KairosDB.
The plugin will accept gzipped and non-gzipped data from Telegraf.

# Configuration
Here is a sample configuration for this plugin. 


```
kairosdb.service.telegraf-plugin=org.kairosdb.telegraf.TelegrafModule
kairosdb.service_folder.telegraf-plugin=lib/telegraf

kairosdb.plugin.telegraf.prefix=telegraph.
```

The optional prefix property adds the prefix string to the beginning of each metric name. 

# Internal Metrics
The plugin writes these metrics to KairosDB:

| Metric Name | Tags | Description |
| ----------- | ---- | ----------- |
| kairosdb.telegraf.ingest_count | status, host | This is the number of metrics ingested from Telegraf. Status is either "success" or "failed". Host is the name of the KairosDB host that reported the metric. |
| kairosdb.telegraf.exception_count | exception, host | This is a count of exceptions when ingesting. The exception tag is the exception name. Host is the name of the KairosDB host that reported the metric. | 