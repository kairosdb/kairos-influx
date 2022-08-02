package org.kairosdb.influxdb;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;

public class Tokenizer
{
	private char[] m_parsedInput;
	private List<Integer> m_tokens = new ArrayList<>();
	private int m_tokenIteratorPos = 0;
	private int m_lastTokenPos = -1;

	public Tokenizer(String input) throws ParseException
	{
		m_parsedInput = new char[input.length()+1];

		CharacterIterator inputIterator = new StringCharacterIterator(input);
		int insertPos = 0;
		boolean startQuote = false;

		while (inputIterator.current() != CharacterIterator.DONE)
		{
			char c = inputIterator.current();

			if (!startQuote && (c == ',' || c == '=' || Character.isWhitespace(c)))
				m_tokens.add(insertPos);

			if (c == '"')
				startQuote = !startQuote;

			if (c == '\\')
			{
				//move past the \ and place the next in the buffer
				c = inputIterator.next();
			}

			m_parsedInput[insertPos] = c;
			inputIterator.next();
			insertPos ++;
		}

		if (startQuote)
		{
			throw new ParseException("Invalid syntax: unterminated double quote");
		}

		m_parsedInput[insertPos] = CharacterIterator.DONE;
		m_tokens.add(insertPos);
	}

	public void next() throws ParseException
	{
		m_lastTokenPos = m_tokens.get(m_tokenIteratorPos);
		m_tokenIteratorPos++;
		if (m_tokenIteratorPos == m_tokens.size())
			throw new ParseException("Premature end of input.");
	}

	public char getChar()
	{
		return m_parsedInput[m_tokens.get(m_tokenIteratorPos)];
	}

	public String getString()
	{
		int stringStart = m_lastTokenPos+1;
		return new String(m_parsedInput, stringStart, m_tokens.get(m_tokenIteratorPos) - stringStart);
	}
}
