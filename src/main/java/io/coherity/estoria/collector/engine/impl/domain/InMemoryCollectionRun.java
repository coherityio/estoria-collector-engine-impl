package io.coherity.estoria.collector.engine.impl.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunStatus;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.spi.CollectionScope;

public class InMemoryCollectionRun implements CollectionRun
{
	private final String runId;
	private final String providerId;
	private final CollectionScope scope;
	private final Instant runStartTime;
	private final Instant runEndTime;
	private final CollectionRunStatus status;
	private final CollectionRunFailure failure;
	private final CollectionRunSummary summary;
	private final List<CollectionResult> results;

	InMemoryCollectionRun(String runId,
		String providerId,
		CollectionScope scope,
		Instant runStartTime,
		Instant runEndTime,
		CollectionRunStatus status,
		CollectionRunFailure failure,
		CollectionRunSummary summary,
		List<CollectionResult> results)
	{
		this.runId = runId;
		this.providerId = providerId;
		this.scope = scope;
		this.runStartTime = runStartTime;
		this.runEndTime = runEndTime;
		this.status = status;
		this.failure = failure;
		this.summary = summary;
		this.results = results != null ? List.copyOf(results) : List.of();
	}

	@Override
	public String getRunId()
	{
		return runId;
	}

	@Override
	public String getProviderId()
	{
		return providerId;
	}

	@Override
	public CollectionScope getCollectionScope()
	{
		return scope;
	}

	@Override
	public Instant getRunStartTime()
	{
		return runStartTime;
	}

	@Override
	public Instant getRunEndTime()
	{
		return runEndTime;
	}

	@Override
	public CollectionRunStatus getStatus()
	{
		return status;
	}

	@Override
	public CollectionRunFailure getCollectionRunFailure()
	{
		return failure;
	}

	@Override
	public CollectionRunSummary getCollectionRunSummary()
	{
		return summary;
	}

	@Override
	public List<CollectionResult> getCollectionResults()
	{
		return results;
	}

	@Override
	public Optional<CollectionResult> getCollectionResult(String collectorId)
	{
		if (collectorId == null || collectorId.isBlank())
		{
			return Optional.empty();
		}
		// Interpret collectorId as the entityType key for the result.
		for (CollectionResult result : results)
		{
			if (collectorId.equals(result.getEntityType()))
			{
				return Optional.of(result);
			}
		}
		return Optional.empty();
	}	
	
	
}