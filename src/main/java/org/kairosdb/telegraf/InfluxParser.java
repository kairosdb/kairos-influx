package org.kairosdb.telegraf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 Parses a line of text in the Influxdb line protocol format (https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/)

 Takes a list measurement names. This list is used to convert fields to tags for the given measurement name. Note that the list
 cannot be null but can be empty.
 */
public class InfluxParser
{
    public ImmutableList<Metric> parseLine(String line)
            throws ParseException
    {
        Builder<Metric> metrics = ImmutableList.builder();

        String[] strings = parseComponents(line);
        if (strings.length < 1){
            return ImmutableList.of();
        }
        Utils.checkParsing(strings.length >= 2, "Invalid syntax. Measurement name and field set is required.");


        // Parse metric name from tags
        String[] nameAndTags = strings[0].split(",");
        Utils.checkParsing(nameAndTags.length > 0 && !nameAndTags[0].isEmpty(), "Invalid sytax. Measurement name was not specified.");
        String metricName = nameAndTags[0];

        // Parse tags
        ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
        for(int i = 1; i < nameAndTags.length; i++)
        {
            String[] tag = nameAndTags[i].split("=");
            Utils.checkParsing(tag.length == 2 && !tag[0].isEmpty() && !tag[1].isEmpty(), "Invalid syntax. Invalid tag set.");
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
            Utils.checkParsing(field.length == 2 && !field[0].isEmpty() && !field[1].isEmpty(), "Invalid syntax. Invalid field set.");

            metrics.add(new Metric(metricName + "." + field[0], tags, parseValue(timestamp, field[1])));
        }

        return metrics.build();
    }

    private String[] parseComponents(String line) throws ParseException
    {
        List<String> components = new ArrayList<>();
        String trimmedLine = line.trim();
        StringBuilder builder = new StringBuilder();
        boolean startQuote = false;
        for (char c : trimmedLine.toCharArray())
        {
            if (c == '"')
            {
                startQuote = !startQuote;
            }
            if (Character.isWhitespace(c) && !startQuote)
            {
                if (builder.length() > 0)
                {
                    // End of component
                    components.add(builder.toString());
                    builder = new StringBuilder();
                }
            }
            else{
                builder.append(c);
            }
        }

        if (startQuote)
        {
            throw new ParseException("Invalid syntax: unterminated double quote");
        }
        else if (builder.length() > 0)
        {
            components.add(builder.toString());
        }
        return components.toArray(new String[0]);
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
}
