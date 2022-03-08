package org.kairosdb.influxdb;

public class Utils
{
	public static void checkParsing(boolean condition, String errorMessage)
			throws ParseException
	{
		if (!condition) {
			throw new ParseException(String.valueOf(errorMessage));
		}
	}
}
