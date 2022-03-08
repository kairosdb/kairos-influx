package org.kairosdb.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 Parses a line of text in the Influxdb line protocol format (https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/)
 <p>
 Takes a list measurement names. This list is used to convert fields to tags for the given measurement name. Note that the list
 cannot be null but can be empty.
 */
public class InfluxParser
{
	private static final Logger logger = LoggerFactory.getLogger(InfluxParser.class);

	private static final String DROP_METRICS_PROP = "kairosdb.plugin.telegraf.dropMetrics";
	private static final String DROP_TAGS_PROP = "kairosdb.plugin.telegraf.dropTags";

	static final String METRICS_DROPPED_METRIC = "kairosdb.telegraf.metrics-dropped.count";
	static final String TAGS_DROPPED_METRIC = "kairosdb.telegraf.tags-dropped.count";

	private final Set<Pattern> dropMetricsRegex = new HashSet<>();
	private final Set<Pattern> dropTagsRegex = new HashSet<>();
	private final MetricWriter writer;

	@Inject
	public InfluxParser(MetricWriter writer)
	{
		this.writer = checkNotNull(writer, "writer must not be null");
	}

	@Inject(optional = true)
	public void setupDroppedMetrics(@Named(DROP_METRICS_PROP) String droppedMetrics)
	{
		createRegexPatterns(droppedMetrics, dropMetricsRegex);
	}

	@Inject(optional = true)
	public void setupDroppedTags(@Named(DROP_TAGS_PROP) String droppedTags)
	{
		createRegexPatterns(droppedTags, dropTagsRegex);
	}

	@SuppressWarnings("Convert2MethodRef")
	public ImmutableList<Metric> parseLine(String line)
			throws ParseException
	{
		int metricsDropped = 0;
		int tagsDropped = 0;

		Builder<Metric> metrics = ImmutableList.builder();

		String[] strings = parseComponents(line, c -> Character.isWhitespace(c));
		if (strings.length < 1)
		{
			return ImmutableList.of();
		}
		Utils.checkParsing(strings.length >= 2, "Invalid syntax. Measurement name and field set is required.");

		// Parse metric name from tags
		String[] nameAndTags = strings[0].split(",");
		Utils.checkParsing(nameAndTags.length > 0 && !nameAndTags[0].isEmpty(), "Invalid syntax. Measurement name was not specified.");
		String metricName = nameAndTags[0];

		// Parse tags
		ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
		for (int i = 1; i < nameAndTags.length; i++)
		{
			String[] tag = parseComponents(nameAndTags[i], c -> c == '=');
			Utils.checkParsing(tag.length == 2 && !tag[0].isEmpty() && !tag[1].isEmpty(), "Invalid syntax. Invalid tag set.");

			if (!drop(tag[0], dropTagsRegex))
			{
				builder.put(tag[0], tag[1]);
			}
			else
			{
				tagsDropped++;
				if (logger.isDebugEnabled())
				{
					logger.debug("Tag {} was dropped because it matched the drop tag regex for metric {}", tag[0], metricName);
				}
			}
		}
		ImmutableSortedMap<String, String> tags = builder.build();

		// Timestamp
		long timestamp = System.currentTimeMillis();
		if (strings.length == 3)
		{
			timestamp = TimeUnit.NANOSECONDS.toMillis(Long.parseLong(strings[2]));
		}

		// Parse Field set
		String[] fieldSets = parseComponents(strings[1], c -> c == ',');
		for (String fieldSet : fieldSets)
		{
			String[] field = parseComponents(fieldSet, c -> c == '=');
			Utils.checkParsing(field.length == 2 && !field[0].isEmpty() && !field[1].isEmpty(), "Invalid syntax. Invalid field set.");

			String name = metricName + "." + field[0];
			if (!drop(name, dropMetricsRegex))
			{
				metrics.add(new Metric(name, tags, parseValue(timestamp, field[1])));
			}
			else
			{
				metricsDropped++;
				if (logger.isDebugEnabled())
				{
					logger.debug("Metric was dropped because it matched the drop metric regex {}", metricName);
				}
			}
		}

		if (metricsDropped > 0)
		{
			writer.write(METRICS_DROPPED_METRIC, new LongDataPoint(System.currentTimeMillis(), metricsDropped));
		}
		if (tagsDropped > 0)
		{
			writer.write(TAGS_DROPPED_METRIC, new LongDataPoint(System.currentTimeMillis(), tagsDropped));
		}

		return metrics.build();

	}

	private String[] parseComponents(String line, Delimiter delimeter)
			throws ParseException
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
			if (delimeter.isDelimeter(c) && !startQuote)
			{
				if (builder.length() > 0)
				{
					// End of component
					components.add(builder.toString());
					builder = new StringBuilder();
				}
			}
			else
			{
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

	private interface Delimiter
	{
		boolean isDelimeter(char c);
	}


	private static void createRegexPatterns(String patterns, Set<Pattern> patternSet)
	{
		if (!isNullOrEmpty(patterns))
		{
			String[] split = patterns.split("\\s*,\\s*");
			for (String pattern : split)
			{
				patternSet.add(Pattern.compile(pattern));
			}
		}
	}

	private static boolean drop(String value, Set<Pattern> patternSet)
	{
		for (Pattern pattern : patternSet)
		{
			if (pattern.matcher(value).matches())
			{
				return true;
			}
		}
		return false;
	}
}
