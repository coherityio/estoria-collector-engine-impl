package io.coherity.estoria.collector.engine.impl.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunStatus;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunEntity;
import io.coherity.estoria.collector.engine.impl.util.JsonSupport;
import io.coherity.estoria.collector.spi.ProviderContext;

public class DaoBackedCollectionRun implements CollectionRun
{
	private final CollectionRunEntity collectionRunEntity;
	private final Map<String, CollectionResult> collectionResultMap;
	
	private final CollectionRunSummary collectionRunSummary;
	private final CollectionRunFailure collectionRunFailure;

	
	public DaoBackedCollectionRun(
			CollectionRunEntity collectionRunEntity, 
			Map<String, CollectionResult> collectionResultMap, 
			CollectionRunSummary collectionRunSummary, 
			CollectionRunFailure collectionRunFailure)
	{
		Validate.notNull(collectionRunEntity, "required: collectionRunEntity");
		Validate.notEmpty(collectionRunEntity.getRunId(), "required: collectionRunEntity.runId");
		this.collectionRunEntity = collectionRunEntity;
		this.collectionResultMap = collectionResultMap;
		this.collectionRunSummary = collectionRunSummary;
		this.collectionRunFailure = collectionRunFailure;
	}
	
	@Override
	public String getRunId()
	{
		return collectionRunEntity.getRunId();
	}

	@Override
	public String getProviderId()
	{
		return collectionRunEntity.getProviderId();
	}

	@Override
	public ProviderContext getProviderContext()
	{
		//TODO: cache result so not multiple calls to deserializer
		return JsonSupport.fromJson(collectionRunEntity.getProviderContext(), ProviderContext.class);
	}

	@Override
	public Instant getRunStartTime()
	{
		return collectionRunEntity.getRunStartTime();
	}

	@Override
	public Instant getRunEndTime()
	{
		return collectionRunEntity.getRunEndTime();
	}

	@Override
	public CollectionRunStatus getStatus()
	{
		if(StringUtils.isNoneEmpty(collectionRunEntity.getStatus()))
		{
			return CollectionRunStatus.valueOf(collectionRunEntity.getStatus().toUpperCase());
		}
		return null;
	}


	@Override
	public CollectionRunFailure getCollectionRunFailure()
	{
		return collectionRunFailure;
	}


	@Override
	public CollectionRunSummary getCollectionRunSummary()
	{
		return collectionRunSummary;
	}
	

	public Optional<CollectionResult> getCollectionResultByEntityType(String entityType)
	{
		return Optional.ofNullable(this.collectionResultMap.get(entityType));
	}


	@Override
	public List<CollectionResult> getCollectionResults()
	{
		return new ArrayList<>(this.collectionResultMap.values());
	}

	
}