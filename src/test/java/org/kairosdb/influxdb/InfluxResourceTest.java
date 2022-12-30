package org.kairosdb.influxdb;

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
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.collectors.LongCollector;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class InfluxResourceTest
{
	@Mock
	private FilterEventBus mockEventBus;
	@Mock
	private Publisher<DataPointEvent> mockPublisher;
	@Mock
	private HttpHeaders mockHeaders;

	private String host;
	private InfluxParser parser;
	private MetricWriter writer;

	@Before
	public void setup() throws UnknownHostException
	{
		MockitoAnnotations.initMocks(this);
		when(mockEventBus.<DataPointEvent>createPublisher(any())).thenReturn(mockPublisher);
		host = "jsabin-desktop";
		writer = new MetricWriter(mockEventBus);
		parser = new InfluxParser();
	}

	@SuppressWarnings("UnstableApiUsage")
	@Test
	public void test() throws IOException
	{
		LongCollector ingestCount = mock(LongCollector.class);
		MetricSourceManager.setCollectorForSource(ingestCount, InfluxStats.class).ingest("success");

		when(mockHeaders.getRequestHeader("Content-Encoding")).thenReturn(null);

		InputStream inputStream = Resources.getResource("examples.txt").openStream();

		InfluxResource resource = new InfluxResource(writer, parser, "influxdb");
		resource.setHostName(host);


		Response response = resource.v1Write(mockHeaders, "db", "ns", inputStream);

		assertThat(response.getStatus()).isEqualTo(204);

		// Verify a few metrics created
		verifyMetric("influxdb.mem.total", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 16773103616L);
		verifyMetric("influxdb.mem.available_percent", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 27.9568771251547);
		verifyMetric("influxdb.system.uptime_format", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, " 5:53");

		// Verify internal metric
		verify(ingestCount).put(211);
	}

	@SuppressWarnings("UnstableApiUsage")
	@Test
	public void testGzippedContent() throws IOException
	{
		LongCollector ingestCount = mock(LongCollector.class);
		MetricSourceManager.setCollectorForSource(ingestCount, InfluxStats.class).ingest("success");
		when(mockHeaders.getRequestHeader("Content-Encoding")).thenReturn(ImmutableList.of("gzip"));

		InputStream inputStream = Resources.getResource("examples.txt.gz").openStream();

		InfluxResource resource = new InfluxResource(writer, parser, "influxdb");
		resource.setHostName(host);

		Response response = resource.v1Write(mockHeaders, "db", "ns",  inputStream);

		assertThat(response.getStatus()).isEqualTo(204);

		// Verify a few metrics created
		verifyMetric("influxdb.mem.total", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 16773103616L);
		verifyMetric("influxdb.mem.available_percent", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, 27.9568771251547);
		verifyMetric("influxdb.system.uptime_format", ImmutableSortedMap.of("host", "jsabin-desktop"), 1547510150000000000L, " 5:53");

		// Verify internal metric
		verify(ingestCount).put(211);
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
}
