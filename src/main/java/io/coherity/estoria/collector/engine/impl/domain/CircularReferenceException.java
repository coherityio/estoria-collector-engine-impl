package io.coherity.estoria.collector.engine.impl.domain;

public class CircularReferenceException extends IllegalStateException
{
	private static final long serialVersionUID = 1L;

	public CircularReferenceException()
	{
		super();
	}
	public CircularReferenceException(String message)
	{
		super(message);
	}
}