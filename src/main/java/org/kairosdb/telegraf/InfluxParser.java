package org.kairosdb.telegraf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;

import java.util.concurrent.TimeUnit;

public class InfluxParser
{
    public ImmutableList<Metric> parseLine(String line)
            throws ParseException
    {
        Builder<Metric> metrics = ImmutableList.builder();

        String[] strings = line.split(" ");
        if (strings.length < 1){
            return ImmutableList.of();
        }
        checkParsing(strings.length >= 2, "Invalid syntax. Measurement name and field set is required.");


        // Parse metric name from tags
        String[] nameAndTags = strings[0].split(",");
        checkParsing(nameAndTags.length > 0 && !nameAndTags[0].isEmpty(), "Invalid sytax. Measurement name was not specified.");
        String metricName = nameAndTags[0];

        ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
        for(int i = 1; i < nameAndTags.length; i++)
        {
            String[] tag = nameAndTags[i].split("=");
            checkParsing(tag.length == 2 && !tag[0].isEmpty() && !tag[1].isEmpty(), "Invalid syntax. Invalid tag set.");
            builder.put(tag[0], tag[1]);
        }
        ImmutableSortedMap<String, String> tags = builder.build();

        // Timestamp
        long timestamp = System.currentTimeMillis();
        if (strings.length == 3)
        {
            timestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong(strings[2]));
        }

        // Parse Field set
        String[] fieldSets = strings[1].split(",");
        for (String fieldSet : fieldSets) {
            String[] field = fieldSet.split("=");
            checkParsing(field.length == 2 && !field[0].isEmpty() && !field[1].isEmpty(), "Invalid syntax. Invalid field set.");

            metrics.add(new Metric(metricName + "." + field[0], tags, parseValue(timestamp, field[1])));
        }

        return metrics.build();
    }

    private DataPoint parseValue(long timestamp, String valueString)
    {
        if (valueString.endsWith("i"))
        {
            String value = valueString.substring(0, valueString.length() - 1);
            return new LongDataPoint(timestamp, Long.parseLong(value));
        }
        else if (valueString.startsWith("\"") && valueString.endsWith("\""))
        {
            return new StringDataPoint(timestamp, valueString.substring(1, valueString.length() - 1));
        }
        else if (valueString.equalsIgnoreCase("t") || valueString.equalsIgnoreCase("true"))
        {
            return new LongDataPoint(timestamp, 1);
        }
        else if (valueString.equalsIgnoreCase("f") || valueString.equalsIgnoreCase("false"))
        {
            return new LongDataPoint(timestamp, 0);
        }
        else
        {
            return new DoubleDataPoint(timestamp, Double.parseDouble(valueString));
        }
    }

    private static void checkParsing(boolean condition, String errorMessage)
            throws ParseException
    {
        if (!condition) {
            throw new ParseException(String.valueOf(errorMessage));
        }
    }
}
