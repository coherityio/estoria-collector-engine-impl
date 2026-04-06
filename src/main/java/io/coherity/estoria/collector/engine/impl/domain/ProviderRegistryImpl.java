package io.coherity.estoria.collector.engine.impl.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;

public class ProviderRegistryImpl implements ProviderRegistry
{
	private Map<String, CloudProvider> loadedProviderMap;
	private Map<String, CollectorRegistry> loadedCollectorRegistryMap;
	
	public ProviderRegistryImpl()
	{
		this.loadedProviderMap = this.loadProviders();
		this.loadedCollectorRegistryMap = this.loadCollectors();
	}

	public ProviderRegistryImpl(Map<String, CloudProvider> loadedProviderMap, Map<String, CollectorRegistry> loadedCollectorRegistryMap)
	{
		this.loadedProviderMap = loadedProviderMap;
		this.loadedCollectorRegistryMap = loadedCollectorRegistryMap;
	}

	protected Map<String, CloudProvider> loadProviders()
	{
		Map<String, CloudProvider> cloudProvidersMap = new HashMap<String, CloudProvider>();
		// Discover CloudProvider implementations via ServiceLoader
		ServiceLoader<CloudProvider> loader = ServiceLoader.load(io.coherity.estoria.collector.spi.CloudProvider.class);
		for (CloudProvider provider : loader)
		{
			cloudProvidersMap.put(provider.getId(), provider);
		}
		return cloudProvidersMap;
	}

	protected Map<String, CollectorRegistry> loadCollectors()
	{
		Map<String, CollectorRegistry> collectorRegistryMap = new HashMap<String, CollectorRegistry>();
		// Discover CloudProvider implementations via ServiceLoader
		ServiceLoader<Collector> loader = ServiceLoader.load(io.coherity.estoria.collector.spi.Collector.class);
		CollectorRegistry collectorRegistry = null;
		for (Collector collector : loader)
		{
			collectorRegistry = collectorRegistryMap.get(collector.getProviderId());
			if(collectorRegistry == null)
			{
				collectorRegistry = new CollectorRegistryImpl();
			}
			collectorRegistry.register(collector);
			collectorRegistryMap.put(collector.getProviderId(), collectorRegistry);
		}
		return collectorRegistryMap;
	}
	
	public List<CloudProvider> getLoadedCloudProviders()
	{
		return new ArrayList<>(this.loadedProviderMap.values());
	}

	public Optional<CollectorRegistry> getLoadedCollectorRegistry(String providerId)
	{
		if(StringUtils.isNotEmpty(providerId) && this.loadedProviderMap.containsKey(providerId) && this.loadedCollectorRegistryMap.containsKey(providerId))
		{
			return Optional.of(this.loadedCollectorRegistryMap.get(providerId));
		}
		return Optional.empty();
	}
	
	
	public Set<String> getMissingCloudProviders()
	{
	    Set<String> difference = new HashSet<>(this.loadedCollectorRegistryMap.keySet());
	    difference.removeAll(this.loadedProviderMap.keySet());
	    return difference;
	}

	@Override
	public Optional<CloudProvider> getLoadedCloudProvider(String providerId)
	{
		if(StringUtils.isNotEmpty(providerId) && this.loadedProviderMap.containsKey(providerId))
		{
			return Optional.of(this.loadedProviderMap.get(providerId));
		}
		return Optional.empty();
	}
	
}