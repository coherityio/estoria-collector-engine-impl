package io.coherity.estoria.collector.engine.impl.cli;

import java.util.HashMap;
import java.util.Map;

import io.coherity.estoria.collector.spi.ProviderContext;

/**
 * Helper for constructing or augmenting CollectionScope instances from CLI arguments.
 */
final class CliProviderContextFactory
{
	private CliProviderContextFactory()
	{
	}

	/**
	 * Overlay provider-arg key=value pairs onto an existing or new CollectionScope.
	 *
	 * All keys are stored in the attributes map. Existing attributes from the
	 * base scope are preserved and overridden by any matching provider-arg keys.
	 */
	static ProviderContext overlayProviderArgs(ProviderContext baseProviderContext, String[] keyValuePairs)
	{
		Map<String, Object> attributes = new HashMap<>();

		if (baseProviderContext != null && baseProviderContext.getAttributes() != null)
		{
			attributes.putAll(baseProviderContext.getAttributes());
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

		return ProviderContext
			.builder()
			.providerId(baseProviderContext != null ? baseProviderContext.getProviderId() : null)
			.attributes(attributes)
			.build();
	}
}
