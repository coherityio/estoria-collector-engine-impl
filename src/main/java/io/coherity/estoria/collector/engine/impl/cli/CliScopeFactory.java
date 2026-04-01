package io.coherity.estoria.collector.engine.impl.cli;

import java.util.HashMap;
import java.util.Map;

import io.coherity.estoria.collector.spi.CollectionScope;
import io.coherity.estoria.collector.spi.ScopeDimension;

/**
 * Helper for constructing or augmenting CollectionScope instances from CLI arguments.
 */
final class CliScopeFactory
{
	private CliScopeFactory()
	{
	}

	/**
	 * Overlay provider-arg key=value pairs onto an existing or new CollectionScope.
	 *
	 * Known dimension keys (matching ScopeDimension names, case-insensitive) are
	 * applied to the dimensions map; all other keys go into attributes.
	 */
	static CollectionScope overlayProviderArgs(CollectionScope baseScope, String[] keyValuePairs)
	{
		Map<ScopeDimension, String> dimensions = new HashMap<>();
		Map<String, String> attributes = new HashMap<>();

		if (baseScope != null)
		{
			if (baseScope.getDimensions() != null)
			{
				dimensions.putAll(baseScope.getDimensions());
			}
			if (baseScope.getAttributes() != null)
			{
				attributes.putAll(baseScope.getAttributes());
			}
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

				ScopeDimension dimension = parseDimension(key);
				if (dimension != null)
				{
					dimensions.put(dimension, value);
				}
				else
				{
					attributes.put(key, value);
				}
			}
		}

		return CollectionScope
			.builder()
			.provider(baseScope != null ? baseScope.getProvider() : null)
			.dimensions(dimensions)
			.attributes(attributes)
			.build();
	}

	private static ScopeDimension parseDimension(String key)
	{
		String upper = key.toUpperCase().replace('-', '_');
		for (ScopeDimension d : ScopeDimension.values())
		{
			if (d.name().equals(upper))
			{
				return d;
			}
		}
		return null;
	}
}
