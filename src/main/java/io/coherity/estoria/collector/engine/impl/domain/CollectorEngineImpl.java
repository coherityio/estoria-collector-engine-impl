package io.coherity.estoria.collector.engine.impl.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;

/**
 * ServiceLoader-based implementation of CollectorEngine.
 * 
 * Discovers and loads CloudProvider implementations from the classpath
 * and creates engine components for orchestrating collection.
 * 
 * Thread-safe singleton for CLI usage via static create() method,
 * or can be instantiated directly for Spring Bean management.
 */
public class CollectorEngineImpl implements CollectorEngine
{
	private static volatile CollectorEngineImpl instance;

	//private CollectorRegistry collectorRegistry;
	private CollectionPlanner collectionPlanner;
	private CollectionExecutor collectionExecutor;
	private SnapshotBuilder snapshotBuilder;
	
	private Map<String, CloudProvider> loadedProviderMap;
	private Map<String, CollectorRegistry> loadedCollectorRegistryMap;


	public CollectorEngineImpl(
			CollectorRegistry collectorRegistry, 
			CollectionPlanner collectionPlanner,
			CollectionExecutor collectionExecutor, 
			SnapshotBuilder snapshotBuilder)
	{
		super();
		//this.collectorRegistry = collectorRegistry;
		this.collectionPlanner = collectionPlanner;
		this.collectionExecutor = collectionExecutor;
		this.snapshotBuilder = snapshotBuilder;
		
		this.loadedProviderMap = this.loadProviders();
		this.loadedCollectorRegistryMap = this.loadCollectors();
	}

	public CollectorEngineImpl()
	{
		//this.collectorRegistry = new CollectorRegistry();
		this.collectionPlanner = new CollectionPlannerImpl(this.loadedCollectorRegistryMap);
		this.collectionExecutor = new CollectionExecutorImpl(this.loadedCollectorRegistryMap);
		this.snapshotBuilder = new ProviderSnapshotBuilderImpl();

		this.loadedProviderMap = this.loadProviders();
		this.loadedCollectorRegistryMap = this.loadCollectors();
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
			if(!collectorRegistryMap.containsKey(collector.getProviderId()))
			{
				collectorRegistry = new CollectorRegistry();
			}
			collectorRegistry.register(collector);
			collectorRegistryMap.put(collector.getProviderId(), collectorRegistry);
		}
		return collectorRegistryMap;
	}
	
	
	/**
	 * Static factory with lazy-initialized singleton for CLI usage.
	 * Thread-safe double-checked locking pattern.
	 */
	public static CollectorEngineImpl getInstance()
	{
		if (instance == null)
		{
			synchronized (CollectorEngineImpl.class)
			{
				if (instance == null)
				{
					instance = new CollectorEngineImpl();
				}
			}
		}
		return instance;
	}

	public CollectionPlanner getPlanner()
	{
		return collectionPlanner;
	}

	public CollectionExecutor getExecutor()
	{
		return collectionExecutor;
	}

	public SnapshotBuilder getSnapshotBuilder()
	{
		return snapshotBuilder;
	}

	@Override
	public List<CloudProvider> getLoadedCloudProviders()
	{
		return new ArrayList<>(this.loadedProviderMap.values());
	}

	@Override
	public List<Collector> getLoadedCollectors(String providerId)
	{
		return new ArrayList<>(this.loadedCollectorRegistryMap.get(providerId).getCollectors());
	}
}
