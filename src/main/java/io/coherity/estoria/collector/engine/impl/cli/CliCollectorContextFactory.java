package io.coherity.estoria.collector.engine.impl.cli;

import java.util.HashMap;
import java.util.Map;

import io.coherity.estoria.collector.spi.CollectorContext;

/**
 * Helper for constructing or augmenting CollectorContext instances from CLI arguments.
 */
final class CliCollectorContextFactory
{
	private CliCollectorContextFactory()
	{
	}

	/**
	 * Overlay collector-arg key=value pairs onto an existing or new CollectorContext.
	 *
	 * All keys are stored in the attributes map. Existing attributes from the
	 * base scope are preserved and overridden by any matching provider-arg keys.
	 */
	static CollectorContext overlayCollectorArgs(CollectorContext baseCollectorContext, String[] keyValuePairs)
	{
		Map<String, Object> attributes = new HashMap<>();

		if (baseCollectorContext != null && baseCollectorContext.getAttributes() != null)
		{
			attributes.putAll(baseCollectorContext.getAttributes());
		}

		if (keyValuePairs != null)
		{
			for (String pair : keyValuePairs)
			{
				if (pair == null || pair.isBlank())
				{
					continue;
				}
				int idx = pair.indexOf('=');
				if (idx <= 0 || idx == pair.length() - 1)
				{
					// ignore malformed entries
					continue;
				}
				String key = pair.substring(0, idx).trim();
				String value = pair.substring(idx + 1).trim();
				if (key.isEmpty())
				{
					continue;
				}

				attributes.put(key, value);
			}
		}

		return CollectorContext
			.builder()
			.attributes(attributes)
			.build();
	}
}
