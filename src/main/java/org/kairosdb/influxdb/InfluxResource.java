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

@Path("api/influx")
public class InfluxResource
{
	private static final Logger logger = LoggerFactory.getLogger(InfluxResource.class);

	public static final String PREFIX_PROP = "kairosdb.influx.prefix";
	public static final String SEPARATOR_PROP = "kairosdb.influx.metric_separator";
	public static final String INCLUDE_BUCKET_PROP = "kairosdb.influx.include_bucket_or_db";

	public static final String INGESTION_COUNT_METRIC = "kairosdb.influx.ingest_count";
	public static final String EXCEPTIONS_METRIC = "kairosdb.influx.exception_count";

	private final InfluxParser m_parser;
	private final MetricWriter m_writer;

	@Inject(optional = true)
	@Named(PREFIX_PROP)
	private String m_metricPrefix;

	@Inject(optional = true)
	@Named(SEPARATOR_PROP)
	private String m_metricSeparator = ".";

	@Inject(optional = true)
	@Named(INCLUDE_BUCKET_PROP)
	private boolean m_useBucket;

	private String m_hostName = "localhost";

	@Inject
	public void setHostName(@Named("HOSTNAME") String hostname)
	{
		m_hostName = hostname;
	}

	@Inject
	public InfluxResource(MetricWriter writer, InfluxParser parser)
	{
		m_writer = checkNotNull(writer, "writer must not be null");
		m_parser = parser;
	}

	public InfluxResource(MetricWriter writer, InfluxParser parser, String metricPrefix)
	{
		this(writer, parser);
		this.m_metricPrefix = metricPrefix;
	}

	@POST
	@Path("/query")
	public Response v1Query()
	{
		return queryInternal();
	}

	@POST
	@Path("/api/v2/query")
	public Response v2Query()
	{
		return queryInternal();
	}

	private Response queryInternal()
	{
		//Some clients try to query the db first so we just return an empty object.
		Response.ResponseBuilder response = Response.status(Response.Status.OK).entity("{}");
		response.header("Content-Type", "application/json;charset=utf-8");
		return response.build();
	}

	private String getPrefix(String bucketOrDB)
	{
		StringBuilder sb = new StringBuilder();
		if (m_metricPrefix != null)
		{
			sb.append(m_metricPrefix);
			sb.append(m_metricSeparator);
		}

		if (m_useBucket)
		{
			sb.append(bucketOrDB).append(m_metricSeparator);
		}

		return sb.toString();
	}

	@SuppressWarnings("UnstableApiUsage")
	@POST
	//Yes this is dumb. We should consume plain text and produce json
	//Influx wasn't strict when creating their server so there are applications
	//that send and accept weird media types
	@Consumes(MediaType.WILDCARD)
	@Produces(MediaType.WILDCARD + "; charset=UTF-8")
	@Path("/write")
	public Response v1Write(@Context HttpHeaders httpheaders,
			@QueryParam("db") String database,
			@QueryParam("precision") String precision,
			InputStream stream)
			throws IOException
	{
		logger.debug("precision: {} db: {} ", precision, database);

		return writeInternal(getPrefix(database), httpheaders, precision, stream);
	}

	@SuppressWarnings("UnstableApiUsage")
	@POST
	//Yes this is dumb. We should consume plain text and produce json
	//Influx wasn't strict when creating their server so there are applications
	//that send and accept weird media types
	@Consumes(MediaType.WILDCARD)
	@Produces(MediaType.WILDCARD + "; charset=UTF-8")
	@Path("/api/v2/write")
	public Response v2write(@Context HttpHeaders httpheaders,
			@QueryParam("bucket") String bucket,
			@QueryParam("precision") String precision,
			InputStream stream)
			throws IOException
	{
		logger.debug("precision: {} bucket: {}", precision, bucket);

		return writeInternal(getPrefix(bucket), httpheaders, precision, stream);
	}


	private Response writeInternal(String metricPrefix, HttpHeaders httpheaders, String precision, InputStream stream) throws IOException
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
					ImmutableList<Metric> metrics = m_parser.parseLine(line, timePrecision);
					for (Metric metric : metrics)
					{
						publishMetric(metricPrefix, metric);
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



	private void publishMetric(String metricPrefix, Metric metric)
	{
		StringBuilder metricNameBuilder = new StringBuilder();
		metricNameBuilder.append(metricPrefix).append(metric.getName());
		m_writer.write(metricNameBuilder.toString(), metric.getTags(), metric.getDataPoint());
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
		ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
		builder.put(tagName, tagValue);
		builder.put("host", m_hostName);
		m_writer.write(metricName, builder.build(), new LongDataPoint(System.currentTimeMillis(), value));
	}
}
