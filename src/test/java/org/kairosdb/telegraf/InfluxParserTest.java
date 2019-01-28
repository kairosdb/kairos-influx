package org.kairosdb.telegraf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.datapoints.StringDataPoint;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class InfluxParserTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private InfluxParser parser = new InfluxParser();

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
        String line = "system,host=localhost uptime_format=\" 5:53\" 1547510150000000000";

        ImmutableSortedMap<String, String> expectedTags = ImmutableSortedMap.of("host", "localhost");
        long expectedTimestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong("1547510150000000000"));
        ImmutableList<Metric> metrics = parser.parseLine(line);

        assertThat(metrics.size(), equalTo(1));
        assertMetric(metrics.get(0), "system.uptime_format", expectedTags, expectedTimestamp, " 5:53");
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
        expectedEx.expectMessage("Invalid sytax. Measurement name was not specified.");

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

    // todo tags are optional

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