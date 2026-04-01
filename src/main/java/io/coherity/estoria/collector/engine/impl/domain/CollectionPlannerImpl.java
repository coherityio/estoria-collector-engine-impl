package io.coherity.estoria.collector.engine.impl.domain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.PlanningException;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.CollectionScope;

public class CollectionPlannerImpl implements CollectionPlanner
{
	private final Map<String, CollectorRegistry> loadedCollectorRegistryMap;

	public CollectionPlannerImpl(Map<String, CollectorRegistry> loadedCollectorRegistryMap)
	{
		this.loadedCollectorRegistryMap = loadedCollectorRegistryMap;
	}

	@Override
	public CollectionPlan plan(
			CloudProvider provider,
			CollectionScope scope,
			Set<String> targetEntityTypes,
			Set<String> skipEntityTypes)
		throws PlanningException
	{
		if (provider == null || scope == null || targetEntityTypes == null || targetEntityTypes.isEmpty())
		{
			throw new IllegalArgumentException("Invalid planning parameters: provider, scope, and targetEntityTypes are required");
		}
		CollectorRegistry collectorRegistry = this.getCollectorRegistry(provider.getId());
		
		Set<String> entityTypeExecutionOrder = new LinkedHashSet<>();
		Set<String> skippedEntityTypes = null;

		try
		{
			for(String targetEntityType : targetEntityTypes)
			{
				entityTypeExecutionOrder.addAll(collectorRegistry.getExecutionOrder(targetEntityType));
			}
		}
		catch(CircularReferenceException cre)
		{
			throw new PlanningException("Failed to determine execution order. Circular reference detected.", cre);
		}
		
		if(skipEntityTypes != null && skipEntityTypes.isEmpty())
		{
			skippedEntityTypes = new HashSet<>();
			skippedEntityTypes.addAll(skipEntityTypes);
			entityTypeExecutionOrder.removeAll(skipEntityTypes);
		}
		
		return CollectionPlan.builder()
			.providerId(provider.getId())
			.collectionScope(scope)
			.entityTypeExecutionOrder(new ArrayList<>(entityTypeExecutionOrder))
			.targetEntityTypes(new HashSet<>(targetEntityTypes))
			.skippedEntityTypes(skippedEntityTypes)
			.build();		
	}
	
	
	@Override
	public CollectionPlan plan(
			CloudProvider provider,
			CollectionScope scope,
			Set<String> targetEntityTypes,
			boolean skipAllDependencies)
		throws PlanningException
	{
		if (provider == null || scope == null || targetEntityTypes == null || targetEntityTypes.isEmpty())
		{
			throw new IllegalArgumentException("Invalid planning parameters: provider, scope, and targetEntityTypes are required");
		}
		CollectorRegistry collectorRegistry = this.getCollectorRegistry(provider.getId());

		Set<String> skippedEntityTypes = null;

		if (skipAllDependencies)
		{
			skippedEntityTypes = new HashSet<>(collectorRegistry.getKnownEntityTypesComplement(targetEntityTypes));
		}
		
		return this.plan(provider, scope, targetEntityTypes, skippedEntityTypes);
	}

	@Override
	public CollectionPlan plan(CloudProvider provider, CollectionScope scope, Set<String> skipEntityTypes) throws PlanningException
	{
		return this.plan(provider, scope, skipEntityTypes);
	}	
	

	@Override
	public CollectionPlan plan(CloudProvider provider, CollectionScope scope) throws PlanningException
	{
		return this.plan(provider, scope, null);
	}
	

	private CollectorRegistry getCollectorRegistry(String providerId)
	{
		CollectorRegistry collectorRegistry = null;
		if(this.loadedCollectorRegistryMap == null)
		{
			throw new IllegalStateException("could not find collector registry");
		}
		collectorRegistry = this.loadedCollectorRegistryMap.get(providerId);
		if(collectorRegistry == null)
		{
			throw new IllegalStateException("could not find collector registry");
		}
		return collectorRegistry;
	}
}
