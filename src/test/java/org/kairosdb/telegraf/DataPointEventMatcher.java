package org.kairosdb.telegraf;

import org.kairosdb.events.DataPointEvent;
import org.mockito.ArgumentMatcher;

public class DataPointEventMatcher implements ArgumentMatcher<DataPointEvent>
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
