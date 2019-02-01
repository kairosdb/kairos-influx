package org.kairosdb.telegraf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TelegrafResourceTest
{
	@Mock
	private FilterEventBus mockEventBus;
	@Mock
	private Publisher<DataPointEvent> mockPublisher;
	@Mock
	private HttpHeaders mockHeaders;

	private String host;

	@Before
	public void setup() throws UnknownHostException
	{
		MockitoAnnotations.initMocks(this);
		when(mockEventBus.<DataPointEvent>createPublisher(any())).thenReturn(mockPublisher);
		host = InetAddress.getLocalHost().getHostName();
	}

	@SuppressWarnings("UnstableApiUsage")
	@Test
	public void test() throws IOException
	{
		when(mockHeaders.getRequestHeader("Content-Encoding")).thenReturn(null);

		InputStream inputStream = Resources.getResource("examples.txt").openStream();

		TelegrafResource resource = new TelegrafResource(mockEventBus, "influxdb.");

		Response response = resource.write(mockHeaders, inputStream);

		assertThat(response.getStatus(), equalTo(204));

		// Verify a few metrics created
		verifyMetric("influxdb.mem.total", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 16773103616L);
		verifyMetric("influxdb.mem.available_percent", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 27.9568771251547);
		verifyMetric("influxdb.system.uptime_format", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, " 5:53");

		// Verify internal metric
		verifyMetric("kairosdb.telegraf.ingest_count", ImmutableSortedMap.of("host", host, "status", "success"), 1547510150000000000L, 211);
	}

	@SuppressWarnings("UnstableApiUsage")
	@Test
	public void testGzippedContent() throws IOException
	{
		when(mockHeaders.getRequestHeader("Content-Encoding")).thenReturn(ImmutableList.of("gzip"));

		InputStream inputStream = Resources.getResource("examples.txt.gz").openStream();

		TelegrafResource resource = new TelegrafResource(mockEventBus, "influxdb.");

		Response response = resource.write(mockHeaders, inputStream);

		assertThat(response.getStatus(), equalTo(204));

		// Verify a few metrics created
		verifyMetric("influxdb.mem.total", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 16773103616L);
		verifyMetric("influxdb.mem.available_percent", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 27.9568771251547);
		verifyMetric("influxdb.system.uptime_format", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, " 5:53");

		// Verify internal metric
		verifyMetric("kairosdb.telegraf.ingest_count", ImmutableSortedMap.of("host", host, "status", "success"), 1547510150000000000L, 211);
	}

	private void verifyMetric(String metricName, ImmutableSortedMap<String, String> tags, long timestamp, long value)
	{
		verify(mockPublisher).post(
				argThat(new DataPointEventMatcher(new DataPointEvent(metricName,
						tags,
						new LongDataPoint(TimeUnit.NANOSECONDS.toMillis(timestamp), value)))));
	}

	private void verifyMetric(String metricName, ImmutableSortedMap<String, String> tags, long timestamp, double value)
	{
		verify(mockPublisher).post(
				argThat(new DataPointEventMatcher(new DataPointEvent(metricName,
						tags,
						new DoubleDataPoint(TimeUnit.NANOSECONDS.toMillis(timestamp), value)))));
	}

	private void verifyMetric(String metricName, ImmutableSortedMap<String, String> tags, long timestamp, String value)
	{
		verify(mockPublisher).post(
				argThat(new DataPointEventMatcher(new DataPointEvent(metricName,
						tags,
						new StringDataPoint(TimeUnit.NANOSECONDS.toMillis(timestamp), value)))));
	}

	private class DataPointEventMatcher implements ArgumentMatcher<DataPointEvent>
	{
		private DataPointEvent event;
		private String errorMessage;

		DataPointEventMatcher(DataPointEvent event)
		{
			this.event = event;
		}

		@Override
		public boolean matches(DataPointEvent dataPointEvent)
		{
			if (!event.getMetricName().equals(dataPointEvent.getMetricName()))
			{
				errorMessage = "Metric names don't match: " + event.getMetricName() + " != " + dataPointEvent.getMetricName();
				return false;
			}
			if (!event.getTags().equals(dataPointEvent.getTags()))
			{
				errorMessage = "Tags don't match: " + event.getTags() + " != " + dataPointEvent.getTags();
				return false;
			}
			if (event.getDataPoint().getDoubleValue() != dataPointEvent.getDataPoint().getDoubleValue())
			{
				errorMessage = "Data points don't match: " + event.getDataPoint().getDoubleValue() + " != " + dataPointEvent.getDataPoint().getDoubleValue();
				return false;
			}
			return true;
		}

		@Override
		public String toString()
		{
			if (errorMessage != null) {
				return errorMessage;
			}
			return "";
		}
	}
}