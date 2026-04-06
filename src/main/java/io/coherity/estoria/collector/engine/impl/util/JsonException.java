package io.coherity.estoria.collector.engine.impl.util;

public class JsonException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public JsonException(String message, Throwable cause)
    {
        super(message, cause);
    }
}