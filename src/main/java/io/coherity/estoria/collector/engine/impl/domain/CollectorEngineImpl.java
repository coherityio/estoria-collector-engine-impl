package io.coherity.estoria.collector.engine.impl.domain;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.CollectorRegistry;

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
//	private static volatile CollectorEngineImpl instance;

	private CollectionPlanner collectionPlanner;
	private CollectionExecutor collectionExecutor;
	private SnapshotBuilder snapshotBuilder;
	
	private ProviderRegistry providerRegistry;
	
	protected CollectorEngineImpl(
			CollectionPlanner collectionPlanner,
			CollectionExecutor collectionExecutor,
			SnapshotBuilder snapshotBuilder,
			ProviderRegistry providerRegistry)
	{
		this.providerRegistry = providerRegistry;
		this.collectionPlanner = collectionPlanner;
		this.collectionExecutor = collectionExecutor;
		this.snapshotBuilder = snapshotBuilder;
	}
	
	public CollectorEngineImpl(
			CollectionPlanner collectionPlanner,
			CollectionExecutor collectionExecutor, 
			SnapshotBuilder snapshotBuilder)
	{
		super();
		this.providerRegistry = new ProviderRegistryImpl();
		this.collectionPlanner = collectionPlanner;
		this.collectionExecutor = collectionExecutor;
		this.snapshotBuilder = snapshotBuilder;
	}

	public CollectorEngineImpl()
	{
		this.providerRegistry = new ProviderRegistryImpl();
		this.collectionPlanner = new CollectionPlannerImpl(this.providerRegistry);
		this.collectionExecutor = new CollectionExecutorImpl(this.providerRegistry);
		this.snapshotBuilder = new ProviderSnapshotBuilderImpl();
	}

//	/**
//	 * Static factory with lazy-initialized singleton for CLI usage.
//	 * Thread-safe double-checked locking pattern.
//	 */
//	public static CollectorEngineImpl getInstance()
//	{
//		if (instance == null)
//		{
//			synchronized (CollectorEngineImpl.class)
//			{
//				if (instance == null)
//				{
//					instance = new CollectorEngineImpl();
//				}
//			}
//		}
//		return instance;
//	}

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
	public Set<CloudProvider> getLoadedCloudProviders()
	{
		return new HashSet<>(this.providerRegistry.getLoadedCloudProviders());
	}

	@Override
	public Optional<CloudProvider> getLoadedCloudProvider(String providerId)
	{
		return this.providerRegistry.getLoadedCloudProvider(providerId);
	}

	@Override
	public Set<String> getRegisteredEntityTypes(String providerId)
	{
		Validate.notEmpty(providerId);
		Set<String> registeredEntityTypeSet = null;
		Optional<CloudProvider> opLoadedCloudProvider = this.providerRegistry.getLoadedCloudProvider(providerId);
		if(opLoadedCloudProvider.isEmpty())
		{
			throw new IllegalStateException("provider: " + providerId + " not found");
		}
		CollectorRegistry collectorRegistry = opLoadedCloudProvider.get().getCollectorRegistry();
		if(collectorRegistry != null)
		{
			registeredEntityTypeSet = collectorRegistry.getRegisteredEntityTypes();
		}
		return registeredEntityTypeSet;
	}

//	@Override
//	public List<Collector> getLoadedCollectors(String providerId)
//	{
//		Optional<CollectorRegistry> opCollectorRegistry = this.providerRegistry.getLoadedCollectorRegistry(providerId);
//		if(opCollectorRegistry.isEmpty())
//		{
//			//empty list
//			return List.of();
//		}
//		return new ArrayList<>(opCollectorRegistry.get().getCollectors());
//	}
}
