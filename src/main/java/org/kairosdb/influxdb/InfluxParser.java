package org.kairosdb.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.InjectProperty;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.influxdb.InfluxResource.BUCKET_TAG_PROP;
import static org.kairosdb.influxdb.InfluxResource.INCLUDE_BUCKET_PROP;

/**
 Parses a line of text in the Influxdb line protocol format (https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/)
 <p>
 Takes a list measurement names. This list is used to convert fields to tags for the given measurement name. Note that the list
 cannot be null but can be empty.
 */
public class InfluxParser
{
	private static final Logger logger = LoggerFactory.getLogger(InfluxParser.class);
	private static final InfluxStats stats = MetricSourceManager.getSource(InfluxStats.class);

	private static final String DROP_METRICS_PROP = "kairosdb.influx.dropMetrics";
	private static final String DROP_TAGS_PROP = "kairosdb.influx.dropTags";

	//static final String METRICS_DROPPED_METRIC = "kairosdb.influx.metrics-dropped.count";
	//static final String TAGS_DROPPED_METRIC = "kairosdb.influx.tags-dropped.count";

	private final Set<Pattern> m_dropMetricsRegex = new HashSet<>();
	private final Set<Pattern> m_dropTagsRegex = new HashSet<>();

	@Inject(optional = true)
	@Named(INCLUDE_BUCKET_PROP)
	private boolean m_useBucket;

	@Inject(optional = true)
	@Named(BUCKET_TAG_PROP)
	private String m_bucketTag;


	@InjectProperty(prop = DROP_METRICS_PROP, optional = true)
	public void setupDroppedMetrics(@Named(DROP_METRICS_PROP) List<String> droppedMetrics)
	{
		createRegexPatterns(droppedMetrics, m_dropMetricsRegex);
	}

	@InjectProperty(prop = DROP_TAGS_PROP, optional = true)
	public void setupDroppedTags(@Named(DROP_TAGS_PROP) List<String> droppedTags)
	{
		createRegexPatterns(droppedTags, m_dropTagsRegex);
	}



	public ImmutableList<Metric> parseLine(String line, TimeUnit precision, String bucket)
			throws ParseException
	{
		int metricsDropped = 0;
		int tagsDropped = 0;

		Builder<Metric> metrics = ImmutableList.builder();

		Tokenizer tokenizer = new Tokenizer(line);
		String metricName = tokenizer.getString();
		Utils.checkParsing(!metricName.isEmpty(), "Invalid syntax. Measurement name was not specified.");
		//check errors


		ImmutableSortedMap.Builder<String, String> tagBuilder = ImmutableSortedMap.naturalOrder();
		if (tokenizer.getChar() == ',')
		{
			//parse out some tags

			while (!Character.isWhitespace(tokenizer.getChar()))
			{
				tokenizer.next();
				Utils.checkParsing(tokenizer.getChar() == '=', "Invalid syntax. Invalid tag set.");

				String tagName = tokenizer.getString();

				tokenizer.next();
				Utils.checkParsing(
						tokenizer.getChar() == ',' || Character.isWhitespace(tokenizer.getChar()),
						"Invalid syntax. Invalid tag set.");

				String tagValue = tokenizer.getString();

				Utils.checkParsing(!tagName.isEmpty() && !tagValue.isEmpty(), "Invalid syntax. Invalid tag set.");

				if (!drop(tagName, m_dropTagsRegex))
				{
					tagBuilder.put(tagName, tagValue);
				}
				else
				{
					tagsDropped++;
					if (logger.isDebugEnabled())
					{
						logger.debug("Tag {} was dropped because it matched the drop tag regex for metric {}", tagName, metricName);
					}
				}

			}
		}

		if (m_useBucket && m_bucketTag != null)
		{
			tagBuilder.put(m_bucketTag, bucket);
		}

		ImmutableSortedMap<String, String> tags = tagBuilder.build();

		ImmutableSortedMap.Builder<String, String> fieldBuilder = ImmutableSortedMap.naturalOrder();

		do
		{
			tokenizer.next();
			Utils.checkParsing(tokenizer.getChar() == '=', "Invalid syntax. Invalid field set.");

			String fieldName = tokenizer.getString();

			tokenizer.next();
			Utils.checkParsing(
					tokenizer.getChar() == ',' || Character.isWhitespace(tokenizer.getChar()) || tokenizer.getChar() == CharacterIterator.DONE,
					"Invalid syntax. Invalid field set.");

			String fieldValue = tokenizer.getString();

			Utils.checkParsing(!fieldName.isEmpty() && !fieldValue.isEmpty(), "Invalid syntax. Invalid field set.");
			fieldBuilder.put(fieldName, fieldValue);

		} while (!Character.isWhitespace(tokenizer.getChar()) && tokenizer.getChar() != CharacterIterator.DONE);

		ImmutableSortedMap<String, String> fields = fieldBuilder.build();

		// Timestamp
		long timestamp = System.currentTimeMillis();

		if (tokenizer.getChar() != CharacterIterator.DONE)
		{
			tokenizer.next();
			String timestampStr = tokenizer.getString();

			if (!timestampStr.isEmpty())
			{
				long parsedTime = Long.parseLong(timestampStr);
				switch (precision)
				{
					case NANOSECONDS:
						timestamp = TimeUnit.NANOSECONDS.toMillis(parsedTime);
						break;
					case MICROSECONDS:
						timestamp = TimeUnit.MICROSECONDS.toMillis(parsedTime);
						break;
					case MILLISECONDS:
						timestamp = parsedTime;
						break;
					case SECONDS:
						timestamp = TimeUnit.SECONDS.toMillis(parsedTime);
						break;
				}
			}
		}


		for (Map.Entry<String, String> fieldEntry : fields.entrySet())
		{
			String name = metricName + "." + fieldEntry.getKey();
			if (!drop(name, m_dropMetricsRegex))
			{
				metrics.add(new Metric(name, tags, parseValue(timestamp, fieldEntry.getValue())));
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
			stats.metricsDropped().put(metricsDropped);
		}
		if (tagsDropped > 0)
		{
			stats.tagsDropped().put(tagsDropped);
		}

		return metrics.build();
	}


	private DataPoint parseValue(long timestamp, String valueString) throws ParseException
	{
		try {
			if (valueString.endsWith("i")) {
				String value = valueString.substring(0, valueString.length() - 1);
				return new LongDataPoint(timestamp, Long.parseLong(value));
			}
			else if (valueString.startsWith("\"") && valueString.endsWith("\"")) {
				return new StringDataPoint(timestamp, valueString.substring(1, valueString.length() - 1));
			}
			else if (valueString.equalsIgnoreCase("t") || valueString.equalsIgnoreCase("true")) {
				return new LongDataPoint(timestamp, 1);
			}
			else if (valueString.equalsIgnoreCase("f") || valueString.equalsIgnoreCase("false")) {
				return new LongDataPoint(timestamp, 0);
			}
			else {
				return new DoubleDataPoint(timestamp, Double.parseDouble(valueString));
			}
		}
		catch (NumberFormatException nfe) {
			throw new ParseException("Unable to parse field value: "+valueString);
		}
	}



	private static void createRegexPatterns(List<String> patterns, Set<Pattern> patternSet)
	{
		for (String pattern : patterns)
		{
			logger.info("Pattern: {}", pattern);
			patternSet.add(Pattern.compile(pattern));
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
