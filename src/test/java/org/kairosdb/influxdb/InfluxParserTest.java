package org.kairosdb.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InfluxParserTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

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
        writer = new MetricWriter(mockEventBus, host);
        parser = new InfluxParser(writer);
    }

    @Test
    public void testSuccess()
            throws ParseException
    {
        String line = "swap,host=localhost,foo=bar total=5,used=0i,free=\"hello\",used_percent=t 1547510150000000000";

        ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
        long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1547510150000000000"));
        ImmutableList<Metric> metrics = parser.parseLine(line);

        assertThat(metrics.size(), equalTo(4));
        assertMetric(metrics.get(0), "swap.total", expectedTags, expectedTimestamp, 5L);
        assertMetric(metrics.get(1), "swap.used", expectedTags, expectedTimestamp, 0L);
        assertMetric(metrics.get(2), "swap.free", expectedTags, expectedTimestamp, "hello");
        assertMetric(metrics.get(3), "swap.used_percent", expectedTags, expectedTimestamp, 1L); // boolean converted to 0 or 1
    }

    @Test
    public void testBoolean()
            throws ParseException
    {
        String line = "swap,host=localhost,foo=bar total=true,used=True,free=f,used_percent=False 1547510150000000000";

        ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
        long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1547510150000000000"));
        ImmutableList<Metric> metrics = parser.parseLine(line);

        assertThat(metrics.size(), equalTo(4));
        assertMetric(metrics.get(0), "swap.total", expectedTags, expectedTimestamp, 1L);
        assertMetric(metrics.get(1), "swap.used", expectedTags, expectedTimestamp, 1L);
        assertMetric(metrics.get(2), "swap.free", expectedTags, expectedTimestamp, 0L);
        assertMetric(metrics.get(3), "swap.used_percent", expectedTags, expectedTimestamp, 0L);
    }

    @Test
    public void testSpaceInQuotes() throws ParseException
    {
        String line = "system,host=localhost uptime_format=\"7 days, 5:46\" 1548718010000000000";

        ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost");
        long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1548718010000000000"));
        ImmutableList<Metric> metrics = parser.parseLine(line);

        assertThat(metrics.size(), equalTo(1));
        assertMetric(metrics.get(0), "system.uptime_format", expectedTags, expectedTimestamp, "7 days, 5:46");
    }

    @Test
    public void testUnterminatedDoubleQuote() throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax: unterminated double quote");

        String line = "system,host=jsabin-desktop uptime_format=\"5:53 1547510150000000000";

        parser.parseLine(line);
    }

    @Test
    public void testNoTimestamp()
            throws ParseException
    {
        String line = "swap,host=localhost,foo=bar total=0i";

        ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost", "foo", "bar");
        long expectedTimestamp = System.currentTimeMillis();
        ImmutableList<Metric> metrics = parser.parseLine(line);

        assertThat(metrics.get(0).getDataPoint().getTimestamp(), is(greaterThanOrEqualTo(expectedTimestamp)));
    }

    @Test
    public void testNoFieldSetInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Measurement name and field set is required.");

        String line = "swap,host=localhost,foo=bar";

        parser.parseLine(line);
    }

    @Test
    public void testNoMeasurementNameInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Measurement name was not specified.");

        String line = ",host=localhost,foo=bar total=0i";

        parser.parseLine(line);
    }

    @Test
    public void testNoTagNameInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Invalid tag set.");

        String line = "swap,=localhost,foo=bar total=0i";

        parser.parseLine(line);
    }

    @Test
    public void testNoTagValueInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Invalid tag set.");

        String line = "swap,host=,foo=bar total=0i";

        parser.parseLine(line);
    }

    @Test
    public void testNoTagEqualSignInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Invalid tag set.");

        String line = "swap,host-localhost,foo=bar total=0i";

        parser.parseLine(line);
    }


    @Test
    public void testNoFieldNameInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Invalid field set.");

        String line = "swap,host=localhost,foo=bar =0i";

        parser.parseLine(line);
    }

    @Test
    public void testNoFieldValueInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Invalid field set.");

        String line = "swap,host=localhost,foo=bar total=";

        parser.parseLine(line);
    }

    @Test
    public void testNoFieldEqualSignInvalid()
            throws ParseException
    {
        expectedEx.expect(ParseException.class);
        expectedEx.expectMessage("Invalid syntax. Invalid field set.");

        String line = "swap,host=localhost,foo=bar total-0i";

        parser.parseLine(line);
    }

    @Test
    public void testDroppedMetricsAndTags() throws ParseException
    {
        parser.setupDroppedMetrics("swap.used.*");
        parser.setupDroppedTags("foo");

        String line = "swap,host=localhost,foo=bar total=true,used=True,free=f,used_percent=False 1547510150000000000";

        ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost");
        long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1547510150000000000"));
        ImmutableList<Metric> metrics = parser.parseLine(line);

        assertThat(metrics.size(), equalTo(2));
        assertMetric(metrics.get(0), "swap.total", expectedTags, expectedTimestamp, 1L);
        assertMetric(metrics.get(1), "swap.free", expectedTags, expectedTimestamp, 0L);
        verifyMetric(InfluxParser.TAGS_DROPPED_METRIC, ImmutableSortedMap.of("host", host), 0, 1);
        verifyMetric(InfluxParser.METRICS_DROPPED_METRIC, ImmutableSortedMap.of("host", host), 0, 2);
    }

    private void verifyMetric(String metricName, ImmutableSortedMap<String, String> tags, long timestamp, long value)
    {
        verify(mockPublisher).post(
              argThat(new DataPointEventMatcher(new DataPointEvent(metricName,
                    tags,
                    new LongDataPoint(TimeUnit.NANOSECONDS.toMillis(timestamp), value)))));
    }

    private void assertMetric(Metric actual, String expectedName, ImmutableSortedMap<String, String> expectedTags, long expectedTimestamp, long expectedValue)
    {
        assertThat(actual.getName(), equalTo(expectedName));
        assertThat(actual.getTags(), equalTo(expectedTags));
        assertThat(actual.getDataPoint().getTimestamp(), equalTo(expectedTimestamp));
        assertThat(actual.getDataPoint().getLongValue(), equalTo(expectedValue));
    }

    private void assertMetric(Metric actual, String expectedName, ImmutableSortedMap<String, String> expectedTags, long expectedTimestamp, String expectedValue)
    {
        assertThat(actual.getName(), equalTo(expectedName));
        assertThat(actual.getTags(), equalTo(expectedTags));
        assertThat(actual.getDataPoint().getTimestamp(), equalTo(expectedTimestamp));
        assertThat(((StringDataPoint)actual.getDataPoint()).getValue(), equalTo(expectedValue));
    }
}