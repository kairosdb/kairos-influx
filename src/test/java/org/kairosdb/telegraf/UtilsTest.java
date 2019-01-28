package org.kairosdb.telegraf;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class UtilsTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Test
	public void testCheckParsingFalse() throws ParseException
	{
		expectedEx.expect(ParseException.class);
		expectedEx.expectMessage("expected error message");

		Utils.checkParsing(false, "expected error message");
	}

	@Test
	public void testCheckParsingTrue() throws ParseException
	{
		try
		{
			Utils.checkParsing(true, "expected error message");
			assertTrue(true);
		}
		catch (Exception e)
		{
			assertFalse("Expected no exception", false);
		}
	}
}