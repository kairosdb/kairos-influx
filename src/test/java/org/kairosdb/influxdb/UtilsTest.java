package org.kairosdb.influxdb;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class UtilsTest
{

	@Test
	public void testCheckParsingFalse() throws ParseException
	{
		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> Utils.checkParsing(false, "expected error message"))
						.withMessage("expected error message");
	}

	@Test
	public void testCheckParsingTrue() throws ParseException
	{
		assertThatCode(() -> Utils.checkParsing(true, "expected error message"))
				.doesNotThrowAnyException();
	}
}