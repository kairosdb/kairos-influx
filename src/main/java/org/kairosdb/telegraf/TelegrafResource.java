package org.kairosdb.telegraf;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang3.StringUtils;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("api/v1/telegraf")
public class TelegrafResource
{
    private static final Logger logger = LoggerFactory.getLogger(TelegrafResource.class);

    private static final String METRIC_PREFIX_PROP = "kairosdb.plugin.telegraf.prefix";
    private static final String METRIC_INGESTION_COUNT = "kairosdb.telegraf.ingest_count";
    private static final String METRIC_EXCEPTIONS = "kairosdb.telegraf.exception_count";

    private final Publisher<DataPointEvent> dataPointPublisher;
    private final String metricPrefix;
    private final String host;

    @Inject
    public TelegrafResource(FilterEventBus eventBus, @Named(METRIC_PREFIX_PROP) String metricPrefix) throws UnknownHostException
    {
        checkNotNull(eventBus, "eventBus must not be null");
        this.metricPrefix = metricPrefix;
        host = InetAddress.getLocalHost().getHostName();
        dataPointPublisher = eventBus.createPublisher(DataPointEvent.class);
    }

//    @POST
////    @Consumes(MediaType.TEXT_PLAIN)
//    @Consumes("application/gzip")
//    @Produces(MediaType.WILDCARD)
//    @Path("/write")
//    public Response write(InputStream gzip)
//            throws IOException
//    {
//        logger.info("*************** in write");
//        String influxString = CharStreams.toString(new InputStreamReader(new GZIPInputStream(gzip), Charsets.UTF_8));
//        logger.info(influxString);
//        return null;
//    }
//

    // todo handle gzip
    // todo how to increment ingestion count

    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.WILDCARD) // todo what should this be?
    @Path("/write")
    public Response write(String data)
    {
        List<String> errors = new ArrayList<>();
        try {
            InfluxParser parser = new InfluxParser();
            String[] lines = data.split("\n");
            for (String line : lines) {
                try
                {
                    ImmutableList<Metric> metrics = parser.parseLine(line);
                    for (Metric metric : metrics)
                    {
                        publishMetric(metric);
                    }
                }
                catch (ParseException e)
                {
                    String msg = "Failed to parse '" + line + "' because " + e.getMessage();
                    logger.error(msg);
                    errors.add(msg);
                    publishInternalMetric(METRIC_EXCEPTIONS, 1, "exception", e.getMessage());
                }
            }
        }
        catch (Throwable e) {
            logger.error("Error processing request: " + data, e);
            publishInternalMetric(METRIC_EXCEPTIONS, 1, "exception", e.getMessage());

            String errorMessage = "{\"error\": " + StringUtils.join(errors, ";") + "}";
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMessage).build();
        }

        publishInternalMetric(METRIC_INGESTION_COUNT, 1);
        return Response.status(Response.Status.NO_CONTENT).build();
    }

    private void publishMetric(Metric metric)
    {
        String metricName = metric.getName();
        if (!Strings.isNullOrEmpty(metricPrefix)){
            metricName = metricPrefix + metricName;
        }
        dataPointPublisher.post(new DataPointEvent(metricName, metric.getTags(), metric.getDataPoint()));
    }

    private void publishInternalMetric(String metricName, long value)
    {
        publishInternalMetric(metricName, value, null, null);
    }

    private void publishInternalMetric(String metricName, long value, String tagName, String tagValue)
    {
        if (logger.isDebugEnabled()) {
            logger.debug("Publishing metric " + metricName + " with value of " + value);
        }

        ImmutableSortedMap<String, String> tags;
        if (!Strings.isNullOrEmpty(tagName)) {
            tags = ImmutableSortedMap.of("host", this.host, tagName, tagValue);
        }
        else {
            tags = ImmutableSortedMap.of("host", this.host);
        }

        dataPointPublisher.post(new DataPointEvent(metricName, tags,
              new LongDataPoint(System.currentTimeMillis(), value)));
    }



    // example Data
//    mem,host=jsabin-desktop available_percent=27.9568771251547,low_free=0i,write_back=0i,write_back_tmp=0i,total=16773103616i,active=10523770880i,wired=0i,shared=265183232i,available=4689235968i,committed_as=23970713600i,huge_pages_total=0i,high_free=0i,high_total=0i,swap_total=0i,vmalloc_total=35184372087808i,inactive=1226072064i,swap_free=0i,vmalloc_chunk=0i,free=1775583232i,buffered=309248000i,dirty=2842624i,huge_page_size=2097152i,huge_pages_free=0i,mapped=1069363200i,page_tables=115609600i,vmalloc_used=0i,cached=2995802112i,slab=399163392i,swap_cached=0i,used=11692470272i,used_percent=69.70964074201783,commit_limit=8386551808i,low_total=0i 1547510150000000000
//    swap,host=jsabin-desktop total=0i,used=0i,free=0i,used_percent=0 1547510150000000000
//    swap,host=jsabin-desktop out=0i,in=0i 1547510150000000000
//    kernel,host=jsabin-desktop entropy_avail=3831i,interrupts=29810121i,context_switches=108093488i,boot_time=1547488927i,processes_forked=57632i 1547510150000000000
//    cpu,cpu=cpu0,host=jsabin-desktop usage_nice=0,usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_system=0.9090909090908099,usage_idle=97.17171717172934,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_user=1.91919191919104 1547510150000000000
//    cpu,cpu=cpu1,host=jsabin-desktop usage_irq=0,usage_guest=0,usage_guest_nice=0,usage_user=3.4000000000003183,usage_system=0.8000000000001251,usage_nice=0,usage_steal=0,usage_idle=95.80000000001746,usage_iowait=0,usage_softirq=0 1547510150000000000
//    cpu,cpu=cpu2,host=jsabin-desktop usage_user=2.694610778442815,usage_idle=96.20758483032932,usage_nice=0,usage_steal=0,usage_system=1.0978043912173696,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_guest=0,usage_guest_nice=0 1547510150000000000
//    cpu,cpu=cpu3,host=jsabin-desktop usage_iowait=0,usage_irq=0,usage_softirq=0,usage_steal=0,usage_system=1.0070493454178386,usage_idle=95.1661631419985,usage_nice=0,usage_user=3.8267875125879587,usage_guest=0,usage_guest_nice=0 1547510150000000000
//    cpu,cpu=cpu4,host=jsabin-desktop usage_guest=0,usage_guest_nice=0,usage_user=1.708542713567303,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_system=0.8040201005025794,usage_idle=97.48743718592983,usage_nice=0,usage_steal=0 1547510150000000000
//    cpu,cpu=cpu5,host=jsabin-desktop usage_guest=0,usage_system=1.3026052104208532,usage_nice=0,usage_iowait=0,usage_softirq=0,usage_steal=0,usage_user=3.406813627254977,usage_idle=95.29058116231278,usage_irq=0,usage_guest_nice=0 1547510150000000000
//    cpu,cpu=cpu6,host=jsabin-desktop usage_user=1.4127144298681837,usage_system=0.40363269424801157,usage_idle=97.5782038344953,usage_nice=0,usage_softirq=0,usage_guest=0,usage_iowait=0.6054490413715872,usage_irq=0,usage_steal=0,usage_guest_nice=0 1547510150000000000
//    cpu,cpu=cpu7,host=jsabin-desktop usage_user=2.8028028028020806,usage_idle=96.29629629630709,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_guest=0,usage_guest_nice=0,usage_system=0.9009009009007907,usage_nice=0,usage_steal=0 1547510150000000000
//    cpu,cpu=cpu-total,host=jsabin-desktop usage_nice=0,usage_irq=0,usage_steal=0,usage_guest_nice=0,usage_user=2.649089767733618,usage_system=0.8788449466416781,usage_idle=96.3967357187752,usage_iowait=0.07532956685492757,usage_softirq=0,usage_guest=0 1547510150000000000
//    disk,device=sdb5,fstype=ext4,host=jsabin-desktop,mode=rw,path=/ used_percent=7.345903950996788,inodes_total=13680640i,inodes_free=13359026i,inodes_used=321614i,total=220407607296i,free=193821155328i,used=15366742016i 1547510150000000000
//    disk,device=sda1,fstype=ext4,host=jsabin-desktop,mode=rw,path=/home inodes_free=29779254i,inodes_used=752330i,total=492124708864i,free=395685220352i,used=71417323520i,used_percent=15.28943150854911,inodes_total=30531584i 1547510150000000000
//    system,host=jsabin-desktop load1=0.77,load5=0.59,load15=0.55,n_cpus=8i,n_users=6i 1547510150000000000
//    diskio,host=jsabin-desktop,name=sda writes=179612i,write_bytes=7298753024i,write_time=11192264i,io_time=1244968i,iops_in_progress=0i,reads=97612i,read_bytes=2207911424i,read_time=2274704i,weighted_io_time=13477092i 1547510150000000000
//    diskio,host=jsabin-desktop,name=sda1 reads=97592i,read_bytes=2207813120i,write_time=11009500i,iops_in_progress=0i,writes=173116i,write_bytes=7298753024i,read_time=2274684i,io_time=1101312i,weighted_io_time=13296136i 1547510150000000000
//    diskio,host=jsabin-desktop,name=sdb read_time=85444i,write_bytes=1259773952i,write_time=126020i,io_time=47320i,weighted_io_time=211456i,iops_in_progress=0i,reads=47402i,writes=29600i,read_bytes=1566051328i 1547510150000000000
//    diskio,host=jsabin-desktop,name=sdb2 write_bytes=0i,weighted_io_time=116i,iops_in_progress=0i,reads=2i,read_bytes=2048i,write_time=0i,io_time=116i,writes=0i,read_time=116i 1547510150000000000
//    diskio,host=jsabin-desktop,name=sdb5 read_bytes=1564300288i,read_time=85104i,write_time=125700i,io_time=46980i,iops_in_progress=0i,reads=47318i,writes=28805i,write_bytes=1259773952i,weighted_io_time=210796i 1547510150000000000
//    system,host=jsabin-desktop uptime=21223i 1547510150000000000
//    system,host=jsabin-desktop uptime_format=" 5:53" 1547510150000000000
//    processes,host=jsabin-desktop blocked=0i,zombies=0i,stopped=0i,dead=0i,paging=0i,running=0i,sleeping=330i,total=330i,unknown=0i,total_threads=1570i,idle=0i 1547510150000000000
}
