package io.coherity.estoria.collector.engine.impl.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.coherity.estoria.collector.spi.ProviderContext;

/**
 * Helper for constructing or augmenting CollectionScope instances from CLI arguments.
 */
final class CliProviderContextFactory
{
	private CliProviderContextFactory()
	{
	}

	static ProviderContext overlayProviderArgs(ProviderContext baseProviderContext, Properties properties)
	{
		Map<String, Object> attributes = new HashMap<>();

		// carry forward existing attributes
		if (baseProviderContext != null && baseProviderContext.getAttributes() != null)
		{
			attributes.putAll(baseProviderContext.getAttributes());
		}

		if (properties != null && !properties.isEmpty())
		{
			for (String name : properties.stringPropertyNames())
			{
				if (name == null || name.isBlank())
				{
					continue;
				}

				String value = properties.getProperty(name);

				if (value == null || value.isBlank())
				{
					// ignore empty values (consistent with your previous behavior)
					continue;
				}

				attributes.put(name.trim(), value.trim());
			}
		}

		return ProviderContext
			.builder()
			.attributes(attributes)
			.build();
	}
	
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
			//.providerId(baseProviderContext != null ? baseProviderContext.getProviderId() : null)
			.attributes(attributes)
			.build();
	}
}
