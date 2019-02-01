package org.kairosdb.telegraf;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.CharStreams;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("api/v1/telegraf")
public class TelegrafResource
{
	private static final Logger logger = LoggerFactory.getLogger(TelegrafResource.class);

	private static final String PROPERTY_PREFIX = "kairosdb.plugin.telegraf.prefix";

	private static final String METRIC_INGESTION_COUNT = "kairosdb.telegraf.ingest_count";
	private static final String METRIC_EXCEPTIONS = "kairosdb.telegraf.exception_count";

	private final Publisher<DataPointEvent> dataPointPublisher;
	private final String host;
	private final InfluxParser parser;

	@Inject(optional = true)
	@Named(PROPERTY_PREFIX)
	private String metricPrefix;


	@Inject
	public TelegrafResource(FilterEventBus eventBus)
			throws UnknownHostException
	{
		checkNotNull(eventBus, "eventBus must not be null");
		host = InetAddress.getLocalHost().getHostName();
		dataPointPublisher = eventBus.createPublisher(DataPointEvent.class);

		parser = new InfluxParser();
	}

	public TelegrafResource(FilterEventBus eventBus, String metricPrefix)
			throws UnknownHostException
	{
		this(eventBus);
		this.metricPrefix = metricPrefix;
	}

	@SuppressWarnings("UnstableApiUsage")
	@POST
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/write")
	public Response write(@Context HttpHeaders httpheaders, InputStream stream)
			throws IOException
	{
		List<String> requestHeader = httpheaders.getRequestHeader("Content-Encoding");
		if (requestHeader != null && requestHeader.contains("gzip"))
		{
			stream = new GZIPInputStream(stream);
		}

		String data = CharStreams.toString(new InputStreamReader(stream));

		List<String> errors = new ArrayList<>();
		int success = 0;
		int failed = 0;
		try
		{
			String[] lines = data.split("\n");
			for (String line : lines)
			{
				try
				{
					ImmutableList<Metric> metrics = parser.parseLine(line);
					for (Metric metric : metrics)
					{
						publishMetric(metric);
						success++;
					}
				}
				catch (ParseException e)
				{
					failed++;
					String msg = "Failed to parse '" + line + "' because " + e.getMessage();
					logger.error(msg);
					errors.add(msg);
					publishInternalMetric(METRIC_EXCEPTIONS, 1, "exception", e.getMessage());
				}
			}
		}
		catch (Throwable e)
		{
			logger.error("Error processing request: " + data, e);
			publishInternalMetric(METRIC_EXCEPTIONS, 1, "exception", e.getMessage());

			String errorMessage = "{\"error\": " + StringUtils.join(errors, ";") + "}";
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMessage).build();
		}

		publishInternalMetric(METRIC_INGESTION_COUNT, success, failed);
		return Response.status(Response.Status.NO_CONTENT).build();
	}

	private void publishMetric(Metric metric)
	{
		String metricName = metric.getName();
		if (!Strings.isNullOrEmpty(metricPrefix))
		{
			metricName = metricPrefix + metricName;
		}
		dataPointPublisher.post(new DataPointEvent(metricName, metric.getTags(), metric.getDataPoint()));
	}

	private void publishInternalMetric(String metricName, int success, int failed)
	{
		publishInternalMetric(metricName, success, "status", "success");

		if (failed > 0)
		{
			publishInternalMetric(metricName, failed, "status", "failed");
		}
	}

	private void publishInternalMetric(String metricName, long value, String tagName, String tagValue)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Publishing metric " + metricName + " with value of " + value);
		}

		ImmutableSortedMap<String, String> tags;
		if (!Strings.isNullOrEmpty(tagName))
		{
			tags = ImmutableSortedMap.of("host", this.host, tagName, tagValue);
		}
		else
		{
			tags = ImmutableSortedMap.of("host", this.host);
		}

		dataPointPublisher.post(new DataPointEvent(metricName, tags,
				new LongDataPoint(System.currentTimeMillis(), value)));
	}
}
