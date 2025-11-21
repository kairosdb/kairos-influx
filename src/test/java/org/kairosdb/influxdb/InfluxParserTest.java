package org.kairosdb.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.collectors.LongCollector;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InfluxParserTest
{
	@Mock
	private FilterEventBus mockEventBus;
	@Mock
	private Publisher<DataPointEvent> mockPublisher;

	private String host;
	private InfluxParser parser;
	private MetricWriter writer;

	@Before
	public void setup()
	{
		MockitoAnnotations.initMocks(this);
		when(mockEventBus.<DataPointEvent>createPublisher(any())).thenReturn(mockPublisher);
		host = "jsabin-desktop";
		writer = new MetricWriter(mockEventBus);
		parser = new InfluxParser();
	}

	@Test
	public void testSuccess()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total=5,used=0i,free=\"hello\",used_percent=t 1547510150000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
		long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1547510150000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.size()).isEqualTo(4);
		assertMetric(metrics.get(1), "swap.total", expectedTags, expectedTimestamp, 5L);
		assertMetric(metrics.get(2), "swap.used", expectedTags, expectedTimestamp, 0L);
		assertMetric(metrics.get(0), "swap.free", expectedTags, expectedTimestamp, "hello");
		assertMetric(metrics.get(3), "swap.used_percent", expectedTags, expectedTimestamp, 1L); // boolean converted to 0 or 1
	}

	@Test
	public void testSeconds()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total=5 154751015";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
		long expectedTimestamp = TimeUnit.SECONDS.toMillis(Long.parseLong("154751015"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.SECONDS, null);

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "swap.total", expectedTags, expectedTimestamp, 5L);
	}

	@Test
	public void testMicroSeconds()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total=5 154751015000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
		long expectedTimestamp = TimeUnit.MICROSECONDS.toMillis(Long.parseLong("154751015000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.MICROSECONDS, null);

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "swap.total", expectedTags, expectedTimestamp, 5L);
	}

	@Test
	public void testMilliSeconds()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total=5 154751015000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
		long expectedTimestamp = Long.parseLong("154751015000");
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.MILLISECONDS, null);

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "swap.total", expectedTags, expectedTimestamp, 5L);
	}

	@Test
	public void testMilliSecondsBucketTag()
			throws ParseException, NoSuchFieldException
	{
		FieldSetter.setField(parser, parser.getClass().getDeclaredField("m_useBucket"), true);
		FieldSetter.setField(parser, parser.getClass().getDeclaredField("m_bucketTag"), "bucket");
		String line = "swap,host=localhost,foo=bar total=5 154751015000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar", "bucket", "bucket_name");
		long expectedTimestamp = Long.parseLong("154751015000");
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.MILLISECONDS, "bucket_name");

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "swap.total", expectedTags, expectedTimestamp, 5L);
	}

	@Test
	public void testBoolean()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total=true,used=True,free=f,used_percent=False 1547510150000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
		long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1547510150000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.size()).isEqualTo(4);
		assertMetric(metrics.get(0), "swap.free", expectedTags, expectedTimestamp, 0L);
		assertMetric(metrics.get(1), "swap.total", expectedTags, expectedTimestamp, 1L);
		assertMetric(metrics.get(2), "swap.used", expectedTags, expectedTimestamp, 1L);
		assertMetric(metrics.get(3), "swap.used_percent", expectedTags, expectedTimestamp, 0L);
	}

	@Test
	public void testSpaceInQuotes() throws ParseException
	{
		String line = "system,host=localhost uptime_format=\"7 days, 5:46\" 1548718010000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost");
		long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1548718010000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "system.uptime_format", expectedTags, expectedTimestamp, "7 days, 5:46");
	}

	@Test
	public void testEscapedSpaceInTags() throws ParseException
	{
		String line = "system,host=local\\ host uptime_format=\"7 days, 5:46\" 1548718010000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "local host");
		long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1548718010000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "system.uptime_format", expectedTags, expectedTimestamp, "7 days, 5:46");
	}

	@Test
	public void testEscapedCommaInTags() throws ParseException
	{
		String line = "system,host=local\\,host uptime_format=\"7 days, 5:46\" 1548718010000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "local,host");
		long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1548718010000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "system.uptime_format", expectedTags, expectedTimestamp, "7 days, 5:46");
	}

	@Test
	public void testEscapedEverything() throws ParseException
	{
		String line = "s\\ ys\\=te\\,m,host=local\\,host uptime_format=\"7 days, 5:46\" 1548718010000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "local,host");
		long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1548718010000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.size()).isEqualTo(1);
		assertMetric(metrics.get(0), "s ys=te,m.uptime_format", expectedTags, expectedTimestamp, "7 days, 5:46");
	}

	@Test
	public void testUnterminatedDoubleQuote() throws ParseException
	{
		String line = "system,host=jsabin-desktop uptime_format=\"5:53 1547510150000000000";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax: unterminated double quote");

	}

	@Test
	public void testNoTimestamp()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total=0i";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
		long expectedTimestamp = System.currentTimeMillis();
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.get(0).getDataPoint().getTimestamp()).isGreaterThanOrEqualTo(expectedTimestamp);
	}

	@Test
	public void testNoFieldSetInvalid()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar ";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Invalid field set.");
	}

	@Test
	public void testNoMeasurementNameInvalid()
			throws ParseException
	{
		String line = ",host=localhost,foo=bar total=0i";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Measurement name was not specified.");
	}

	@Test
	public void testNoTagNameInvalid()
			throws ParseException
	{
		String line = "swap,=localhost,foo=bar total=0i";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Invalid tag set.");
	}

	@Test
	public void testNoTagValueInvalid()
			throws ParseException
	{
		String line = "swap,host=,foo=bar total=0i";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Invalid tag set.");
	}

	@Test
	public void testNoTagEqualSignInvalid()
			throws ParseException
	{
		String line = "swap,host-localhost,foo=bar total=0i";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Invalid tag set.");
	}


	@Test
	public void testNoFieldNameInvalid()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar =0i";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Invalid field set.");
	}

	@Test
	public void testNoFieldValueInvalid()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total=";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Invalid field set.");
	}

	@Test
	public void testNoFieldEqualSignInvalid()
			throws ParseException
	{
		String line = "swap,host=localhost,foo=bar total-0i";

		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parser.parseLine(line, TimeUnit.NANOSECONDS, null))
				.withMessage("Invalid syntax. Invalid field set.");
	}

	@Test
	public void testDroppedMetricsAndTags() throws ParseException
	{
		LongCollector metricsDropped = mock(LongCollector.class);
		LongCollector tagsDropped = mock(LongCollector.class);

		MetricSourceManager.setCollectorForSource(metricsDropped, InfluxStats.class).metricsDropped();
		MetricSourceManager.setCollectorForSource(tagsDropped, InfluxStats.class).tagsDropped();

		parser.setupDroppedMetrics(Arrays.asList("swap.used.*"));
		parser.setupDroppedTags(Arrays.asList("foo"));

		String line = "swap,host=localhost,foo=bar total=true,used=True,free=f,used_percent=False 1547510150000000000";

		ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost");
		long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1547510150000000000"));
		ImmutableList<Metric> metrics = parser.parseLine(line, TimeUnit.NANOSECONDS, null);

		assertThat(metrics.size()).isEqualTo(2);
		assertMetric(metrics.get(1), "swap.total", expectedTags, expectedTimestamp, 1L);
		assertMetric(metrics.get(0), "swap.free", expectedTags, expectedTimestamp, 0L);
		verify(metricsDropped).put(2);
		verify(tagsDropped).put(1);
	}

	private void assertMetric(Metric actual, String expectedName, ImmutableSortedMap<String, String> expectedTags, long expectedTimestamp, long expectedValue)
	{
		assertThat(actual.getName()).isEqualTo(expectedName);
		assertThat(actual.getTags()).isEqualTo(expectedTags);
		assertThat(actual.getDataPoint().getTimestamp()).isEqualTo(expectedTimestamp);
		assertThat(actual.getDataPoint().getLongValue()).isEqualTo(expectedValue);
	}

	private void assertMetric(Metric actual, String expectedName, ImmutableSortedMap<String, String> expectedTags, long expectedTimestamp, String expectedValue)
	{
		assertThat(actual.getName()).isEqualTo(expectedName);
		assertThat(actual.getTags()).isEqualTo(expectedTags);
		assertThat(actual.getDataPoint().getTimestamp()).isEqualTo(expectedTimestamp);
		assertThat(((StringDataPoint)actual.getDataPoint()).getValue()).isEqualTo(expectedValue);
	}
}