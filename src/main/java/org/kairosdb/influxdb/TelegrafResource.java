package org.kairosdb.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang3.StringUtils;
import org.kairosdb.core.datapoints.LongDataPoint;
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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

@Path("api/v1/telegraf")
public class TelegrafResource
{
	private static final Logger logger = LoggerFactory.getLogger(TelegrafResource.class);

	private static final String PREFIX_PROP = "kairosdb.plugin.telegraf.prefix";

	private static final String INGESTION_COUNT_METRIC = "kairosdb.telegraf.ingest_count";
	private static final String EXCEPTIONS_METRIC = "kairosdb.telegraf.exception_count";

	private final InfluxParser parser;
	private final MetricWriter writer;

	@Inject(optional = true)
	@Named(PREFIX_PROP)
	private String metricPrefix;

	@Inject
	public TelegrafResource(MetricWriter writer, InfluxParser parser)
	{
		this.writer = checkNotNull(writer, "writer must not be null");
		this.parser = parser;
	}

	public TelegrafResource(MetricWriter writer, InfluxParser parser, String metricPrefix)
	{
		this(writer, parser);
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
					publishInternalMetric(EXCEPTIONS_METRIC, 1, "exception", e.getMessage());
				}
			}
		}
		catch (Throwable e)
		{
			logger.error("Error processing request: " + data, e);
			publishInternalMetric(EXCEPTIONS_METRIC, 1, "exception", e.getMessage());

			String errorMessage = "{\"error\": " + StringUtils.join(errors, ";") + "}";
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMessage).build();
		}

		publishInternalMetric(INGESTION_COUNT_METRIC, success, failed);
		return Response.status(Response.Status.NO_CONTENT).build();
	}

	private void publishMetric(Metric metric)
	{
		writer.write(isNullOrEmpty(metricPrefix) ? metric.getName() : metricPrefix + metric.getName(), metric.getTags(), metric.getDataPoint());
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
		writer.write(metricName, ImmutableSortedMap.of(tagName, tagValue), new LongDataPoint(System.currentTimeMillis(), value));
	}
}
