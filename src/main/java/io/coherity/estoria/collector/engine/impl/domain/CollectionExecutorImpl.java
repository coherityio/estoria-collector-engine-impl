package io.coherity.estoria.collector.engine.impl.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunStatus;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.ExecutionException;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunH2Dao;
import lombok.extern.slf4j.Slf4j;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.CollectionScope;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorRequest;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderSession;

/**
	 * Thread-safe implementation of CollectionExecutor.
	 */
@Slf4j
public class CollectionExecutorImpl implements CollectionExecutor
{

	private final Map<String, CollectorRegistry> loadedCollectorRegistryMap;
	private final CollectionRunDao collectionRunDao;
	private final CollectionResultDao collectionResultDao;
	private final CollectedEntityDao collectedEntityDao;

	public CollectionExecutorImpl(Map<String, CollectorRegistry> loadedCollectorRegistryMap)
	{
		this(loadedCollectorRegistryMap, new CollectionRunH2Dao(), new CollectionResultH2Dao(),
			new CollectedEntityH2Dao());
	}

	public CollectionExecutorImpl(Map<String, CollectorRegistry> loadedCollectorRegistryMap,
		CollectionRunDao collectionRunDao,
		CollectionResultDao collectionResultDao,
		CollectedEntityDao collectedEntityDao)
	{
		this.loadedCollectorRegistryMap = loadedCollectorRegistryMap;
		this.collectionRunDao = collectionRunDao;
		this.collectionResultDao = collectionResultDao;
		this.collectedEntityDao = collectedEntityDao;
	}

	@Override
	public CollectionRun collect(CollectionPlan collectionPlan, ProviderSession session) throws ExecutionException
	{
		if (log.isDebugEnabled())
		{
			log.debug("Starting collection run for providerId={} with plan={} and session={}",
				collectionPlan != null ? collectionPlan.getProviderId() : null,
				collectionPlan,
				session);
		}

		if (collectionPlan == null)
		{
			throw new IllegalArgumentException("collectionPlan cannot be null");
		}

		if (collectionPlan.getProviderId() == null || collectionPlan.getProviderId().isBlank())
		{
			throw new IllegalArgumentException("providerId cannot be null or empty");
		}

		if (collectionPlan.getCollectionScope() == null)
		{
			throw new IllegalArgumentException("collectionScope cannot be null");
		}

		if (session == null)
		{
			throw new IllegalArgumentException("session cannot be null");
		}

		List<String> executionOrder = collectionPlan.getEntityTypeExecutionOrder();
		if (executionOrder == null || executionOrder.isEmpty())
		{
			throw new ExecutionException("Collection plan has no entity types");
		}

		CollectorRegistry collectorRegistry = getCollectorRegistry(collectionPlan.getProviderId());
		CollectionScope scope = collectionPlan.getCollectionScope();
		Set<String> skippedTypes = collectionPlan.getSkippedEntityTypes() != null
			? new HashSet<>(collectionPlan.getSkippedEntityTypes())
			: Set.of();
		Set<String> targetTypes = collectionPlan.getTargetEntityTypes() != null
			? new HashSet<>(collectionPlan.getTargetEntityTypes())
			: null;
		Integer planPageSize = collectionPlan.getPageSize() != null ? collectionPlan.getPageSize() : CollectionPlan.PAGE_SIZE_ALL;

		Instant runStartTime = Instant.now();
		String runId = UUID.randomUUID().toString();
		log.info("Collection run {} started for providerId={} with {} entity types", runId,
			collectionPlan.getProviderId(), executionOrder.size());

		CollectionRunEntity runEntity = CollectionRunEntity.builder()
			.runId(runId)
			.providerId(collectionPlan.getProviderId())
			.collectionScope(scope.toString())
			.status(CollectionRunStatus.RUNNING.name())
			.runStartTime(runStartTime)
			.runEndTime(null)
			.totalCollectorCount(0L)
			.successfulCollectorCount(0L)
			.failedCollectorCount(0L)
			.totalEntityCount(0L)
			.failureMessage(null)
			.failureExceptionClass(null)
			.build();
		collectionRunDao.save(runEntity);

		long totalCollectorCount = 0L;
		long successfulCollectorCount = 0L;
		long failedCollectorCount = 0L;
		long totalEntityCount = 0L;
		boolean anySuccess = false;
		boolean anyFailure = false;
		String runFailureMessage = null;
		String runFailureExceptionClass = null;

		for (String entityType : executionOrder)
		{
			if (entityType == null || entityType.isBlank())
			{
				log.warn("Encountered blank entityType in execution order for runId={}; skipping", runId);
				continue;
			}

			if (skippedTypes.contains(entityType))
			{
				log.info("Skipping entityType={} for runId={} due to collectionPlan.skippedEntityTypes", entityType,
					runId);
				continue;
			}

			if (targetTypes != null && !targetTypes.isEmpty() && !targetTypes.contains(entityType))
			{
				log.debug("EntityType={} not in targetTypes for runId={}; skipping", entityType, runId);
				continue;
			}

			Collector collector = collectorRegistry.getCollector(entityType);
			if (collector == null)
			{
				// Record a failed result for missing collector and continue.
				log.warn("No collector registered for entityType={} and providerId={}; marking result FAILED and continuing",
					entityType, collectionPlan.getProviderId());
				String resultId = UUID.randomUUID().toString();
				Instant resultStart = Instant.now();
				Instant resultEnd = resultStart;
				String msg = "No collector registered for entity type '" + entityType + "'";
				CollectionResultEntity resultEntity = CollectionResultEntity.builder()
					.resultId(resultId)
					.runId(runId)
					.collectorId("<missing>")
					.entityType(entityType)
					.status(CollectionRunStatus.FAILED.name())
					.entityCount(0L)
					.collectionStartTime(resultStart)
					.collectionEndTime(resultEnd)
					.failureMessage(msg)
					.failureExceptionClass(ExecutionException.class.getName())
					.build();
				collectionResultDao.save(resultEntity);

				totalCollectorCount++;
				failedCollectorCount++;
				anyFailure = true;
				if (runFailureMessage == null)
				{
					runFailureMessage = msg;
					runFailureExceptionClass = ExecutionException.class.getName();
				}
				continue;
			}

			String resultId = UUID.randomUUID().toString();
			Instant resultStart = Instant.now();
			long entityCount = 0L;
			long entityOrdinal = 0L;
			String failureMessage = null;
			String failureExceptionClass = null;
			CollectionRunStatus resultStatus = CollectionRunStatus.SUCCESS;

			Optional<String> cursorToken = Optional.empty();
			try
			{
				log.debug("Starting collection for entityType={} in runId={}", entityType, runId);
				while (true)
				{
					CollectorRequest request = CollectorRequest.builder()
						.scope(scope)
						.cursorToken(cursorToken)
						.pageSize(planPageSize)
						.build();

					CollectorCursor cursor = collector.collect(request);
					if (cursor == null)
					{
						log.warn("Collector {} returned null cursor for entityType={} in runId={}",
							collector.getClass().getName(), entityType, runId);
						break;
					}

					List<CloudEntity> entities = cursor.getEntities();
					if (entities != null && !entities.isEmpty())
					{
						List<CollectedEntityEntity> batch = new ArrayList<>(entities.size());
						for (CloudEntity cloudEntity : entities)
						{
							if (log.isTraceEnabled())
							{
								log.trace("Processing entity of type={} in runId={}", cloudEntity.getEntityType(),
									runId);
							}
							EntityIdentifier identifier = cloudEntity.getEntityIdentifier();
							String entityId = (identifier != null) ? identifier.getId() : null;
							String storedEntityType = cloudEntity.getEntityType() != null
								? cloudEntity.getEntityType()
								: entityType;
							String payloadJson = cloudEntity.getRawPayload() != null
								? cloudEntity.getRawPayload().toString()
								: null;

							CollectedEntityEntity stored = CollectedEntityEntity.builder()
								.resultId(resultId)
								.entityOrdinal(entityOrdinal++)
								.entityId(entityId)
								.entityType(storedEntityType)
								.payloadJson(payloadJson)
								.build();
							batch.add(stored);
							entityCount++;
						}
						collectedEntityDao.saveAll(batch);
					}

					Optional<String> next = cursor.getNextCursorToken();
					if (next == null || next.isEmpty())
					{
						break;
					}
					cursorToken = next;
				}
			}
			catch (Exception e)
			{
				log.error("Collection failed for entityType={} in runId={}: {}", entityType, runId,
					e.getMessage(), e);
				resultStatus = CollectionRunStatus.FAILED;
				failureMessage = e.getMessage();
				failureExceptionClass = e.getClass().getName();
				anyFailure = true;
				if (runFailureMessage == null)
				{
					runFailureMessage = failureMessage;
					runFailureExceptionClass = failureExceptionClass;
				}
			}

			Instant resultEnd = Instant.now();
			CollectionResultEntity resultEntity = CollectionResultEntity.builder()
				.resultId(resultId)
				.runId(runId)
				.collectorId(collector.getClass().getName())
				.entityType(entityType)
				.status(resultStatus.name())
				.entityCount(entityCount)
				.collectionStartTime(resultStart)
				.collectionEndTime(resultEnd)
				.failureMessage(failureMessage)
				.failureExceptionClass(failureExceptionClass)
				.build();
			collectionResultDao.save(resultEntity);

			totalCollectorCount++;
			if (resultStatus == CollectionRunStatus.SUCCESS)
			{
				successfulCollectorCount++;
				anySuccess = true;
				totalEntityCount += entityCount;
			}
			else
			{
				failedCollectorCount++;
			}
		}

		Instant runEndTime = Instant.now();
		CollectionRunStatus runStatus;
		if (anyFailure)
		{
			runStatus = anySuccess ? CollectionRunStatus.PARTIAL_SUCCESS : CollectionRunStatus.FAILED;
		}
		else
		{
			runStatus = CollectionRunStatus.SUCCESS;
		}

		runEntity.setStatus(runStatus.name());
		runEntity.setRunEndTime(runEndTime);
		runEntity.setTotalCollectorCount(totalCollectorCount);
		runEntity.setSuccessfulCollectorCount(successfulCollectorCount);
		runEntity.setFailedCollectorCount(failedCollectorCount);
		runEntity.setTotalEntityCount(totalEntityCount);
		runEntity.setFailureMessage(runFailureMessage);
		runEntity.setFailureExceptionClass(runFailureExceptionClass);
		collectionRunDao.save(runEntity);
		log.info("Collection run {} completed with status={} (totalCollectors={}, successes={}, failures={}, totalEntities={})",
			runId, runStatus, totalCollectorCount, successfulCollectorCount, failedCollectorCount, totalEntityCount);

		CollectionRunSummary summary = CollectionRunSummary.builder()
			.totalCollectorCount(totalCollectorCount)
			.successfulCollectorCount(successfulCollectorCount)
			.failedCollectorCount(failedCollectorCount)
			.totalEntityCount(totalEntityCount)
			.build();

		CollectionRunFailure failure = null;
		if (runFailureMessage != null && !runFailureMessage.isBlank())
		{
			failure = CollectionRunFailure.builder()
				.message(runFailureMessage)
				.exceptionClassName(runFailureExceptionClass)
				.build();
		}

		List<CollectionResult> results = new ArrayList<>();
		for (CollectionResultEntity resultEntity : collectionResultDao.findByRunId(runId))
		{
			results.add(new DaoCollectionResult(resultEntity.getResultId(), collectionResultDao, collectedEntityDao));
		}

		return new InMemoryCollectionRun(runId, collectionPlan.getProviderId(), scope, runStartTime, runEndTime,
			runStatus, failure, summary, results);
	}

	private CollectorRegistry getCollectorRegistry(String providerId) throws ExecutionException
	{
		if (loadedCollectorRegistryMap == null || loadedCollectorRegistryMap.isEmpty())
		{
			throw new ExecutionException("No collector registries loaded");
		}

		CollectorRegistry collectorRegistry = loadedCollectorRegistryMap.get(providerId);
		if (collectorRegistry == null)
		{
			throw new ExecutionException("No collector registry found for provider '" + providerId + "'");
		}
		return collectorRegistry;
	}


}
