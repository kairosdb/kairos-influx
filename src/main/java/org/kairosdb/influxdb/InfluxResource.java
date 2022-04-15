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

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

@Path("api/v1/influx")
public class InfluxResource
{
	private static final Logger logger = LoggerFactory.getLogger(InfluxResource.class);

	private static final String PREFIX_PROP = "kairosdb.influx.prefix";

	private static final String INGESTION_COUNT_METRIC = "kairosdb.influx.ingest_count";
	private static final String EXCEPTIONS_METRIC = "kairosdb.influx.exception_count";

	private final InfluxParser parser;
	private final MetricWriter writer;

	@Inject(optional = true)
	@Named(PREFIX_PROP)
	private String metricPrefix;

	@Inject
	public InfluxResource(MetricWriter writer, InfluxParser parser)
	{
		this.writer = checkNotNull(writer, "writer must not be null");
		this.parser = parser;
	}

	public InfluxResource(MetricWriter writer, InfluxParser parser, String metricPrefix)
	{
		this(writer, parser);
		this.metricPrefix = metricPrefix;
	}

	@SuppressWarnings("UnstableApiUsage")
	@POST
	//Yes this is dumb. We should consume plain text and produce json
	//Influx wasn't strict when creating their server so there are applications
	//that send and accept weird media types
	@Consumes(MediaType.WILDCARD)
	@Produces(MediaType.WILDCARD + "; charset=UTF-8")
	//@Produces(MediaType.APPLICATION_JSON +"; charset=UTF-8")
	@Path("/write")
	public Response write(@Context HttpHeaders httpheaders,
			@QueryParam("bucket") String bucket,
			@QueryParam("org") String org,
			@QueryParam("precision") String precision,
			InputStream stream)
			throws IOException
	{
		List<String> requestHeader = httpheaders.getRequestHeader("Content-Encoding");
		if (requestHeader != null && requestHeader.contains("gzip"))
		{
			stream = new GZIPInputStream(stream);
		}
		TimeUnit timePrecision = TimeUnit.NANOSECONDS;
		if (precision != null)
		{
			if ("ns".equals(precision))
				timePrecision = TimeUnit.NANOSECONDS;
			else if ("ms".equals(precision))
				timePrecision = TimeUnit.MILLISECONDS;
			else if ("s".equals(precision))
				timePrecision = TimeUnit.SECONDS;
			else if ("us".equals(precision))
				timePrecision = TimeUnit.MICROSECONDS;
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
					ImmutableList<Metric> metrics = parser.parseLine(line, timePrecision);
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

			String errorMessage = "{\"code\": \"internal error\", \"message\": \"" + StringUtils.join(errors, ";") + "\"}";
			Response.ResponseBuilder response = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMessage);
			response.header("Content-Type", "application/json;charset=utf-8");
			return response.build();
		}

		publishInternalMetric(INGESTION_COUNT_METRIC, success, failed);
		if (failed != 0)
		{
			String errorMessage = "{\"code\": \"invalid\", \"message\": \"partial write error (" + success + " written): " + StringUtils.join(errors, ";") + "\"}";
			Response.ResponseBuilder response = Response.status(Response.Status.BAD_REQUEST).entity(errorMessage);
			response.header("Content-Type", "application/json;charset=utf-8");
			return response.build();
		}
		else {
			return Response.status(Response.Status.NO_CONTENT).build();
		}
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
