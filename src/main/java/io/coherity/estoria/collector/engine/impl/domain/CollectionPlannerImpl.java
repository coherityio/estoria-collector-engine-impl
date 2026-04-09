package io.coherity.estoria.collector.engine.impl.domain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.PlanningException;
import io.coherity.estoria.collector.spi.CircularReferenceException;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorRegistry;

public class CollectionPlannerImpl implements CollectionPlanner
{
	private final ProviderRegistry providerRegistry;

	public CollectionPlannerImpl(ProviderRegistry providerRegistry)
	{
		this.providerRegistry = providerRegistry;
	}

	@Override
	public CollectionPlan plan(
			String providerId,
			Set<String> targetEntityTypes,
			Set<String> skipEntityTypes, 
			CollectorContext collectorContext)
		throws PlanningException
	{
		Validate.notEmpty(providerId, "required: providerId");
		Validate.notEmpty(targetEntityTypes, "required: targetEntityTypes");
		Optional<CollectorRegistry> opCollectorRegistry = this.providerRegistry.getLoadedCollectorRegistry(providerId);
		if(opCollectorRegistry.isEmpty())
		{
			throw new IllegalStateException("could not find collector registry");
		}
		
		Set<String> entityTypeExecutionOrder = new LinkedHashSet<>();
		Set<String> skippedEntityTypes = null;

		try
		{
			for(String targetEntityType : targetEntityTypes)
			{
				entityTypeExecutionOrder.addAll(opCollectorRegistry.get().getExecutionOrder(targetEntityType));
			}
		}
		catch(CircularReferenceException cre)
		{
			throw new PlanningException("Failed to determine execution order. Circular reference detected.", cre);
		}
		
		if (skipEntityTypes != null && !skipEntityTypes.isEmpty())
		{
			skippedEntityTypes = new HashSet<>();
			skippedEntityTypes.addAll(skipEntityTypes);
			entityTypeExecutionOrder.removeAll(skipEntityTypes);
		}
		
		return CollectionPlan.builder()
			.providerId(providerId)
			.collectorContext(collectorContext)
			.entityTypeExecutionOrder(new ArrayList<>(entityTypeExecutionOrder))
			.targetEntityTypes(new HashSet<>(targetEntityTypes))
			.skippedEntityTypes(skippedEntityTypes)
			.build();		
	}
	
	@Override
	public CollectionPlan plan(
			String providerId,
			Set<String> targetEntityTypes,
			boolean skipAllDependencies, 
			CollectorContext collectorContext)
		throws PlanningException
	{
		Validate.notEmpty(providerId, "required: providerId");
		Validate.notEmpty(targetEntityTypes, "required: targetEntityTypes");
		Optional<CollectorRegistry> opCollectorRegistry = this.providerRegistry.getLoadedCollectorRegistry(providerId);
		if(opCollectorRegistry.isEmpty())
		{
			throw new IllegalStateException("could not find collector registry");
		}

		Set<String> skippedEntityTypes = null;

		if (skipAllDependencies)
		{
			skippedEntityTypes = new HashSet<>(opCollectorRegistry.get().getKnownEntityTypesComplement(targetEntityTypes));
		}
		
		return this.plan(providerId, targetEntityTypes, skippedEntityTypes, collectorContext);
	}

	@Override
	public CollectionPlan plan(String providerId, Set<String> skipEntityTypes, CollectorContext collectorContext) throws PlanningException
	{
		Optional<CollectorRegistry> opCollectorRegistry = this.providerRegistry.getLoadedCollectorRegistry(providerId);
		if(opCollectorRegistry.isEmpty())
		{
			throw new IllegalStateException("could not find collector registry");
		}

		Set<String> effectiveTargetEntityTypes = new HashSet<>(opCollectorRegistry.get().getRegisteredEntityTypes());
		if (skipEntityTypes != null && !skipEntityTypes.isEmpty())
		{
			effectiveTargetEntityTypes.removeAll(skipEntityTypes);
		}
		if (effectiveTargetEntityTypes.isEmpty())
		{
			throw new IllegalArgumentException("Invalid planning parameters: no target entity types remain after applying skipEntityTypes");
		}
		return this.plan(providerId, effectiveTargetEntityTypes, skipEntityTypes, collectorContext);
	}

	@Override
	public CollectionPlan plan(String providerId, CollectorContext collectorContext) throws PlanningException
	{
		Optional<CollectorRegistry> opCollectorRegistry = this.providerRegistry.getLoadedCollectorRegistry(providerId);
		if(opCollectorRegistry.isEmpty())
		{
			throw new IllegalStateException("could not find collector registry");
		}
		Set<String> targetEntityTypes = new HashSet<>(opCollectorRegistry.get().getRegisteredEntityTypes());
		if (targetEntityTypes.isEmpty())
		{
			throw new IllegalStateException("No collectors registered for provider '" + providerId + "'");
		}
		return this.plan(providerId, targetEntityTypes, false, collectorContext);
	}
	
}
