package io.coherity.estoria.collector.engine.impl.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunStatus;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
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
import io.coherity.estoria.collector.engine.impl.util.JsonSupport;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorRequestParameters;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import io.coherity.estoria.collector.spi.ProviderSession;
import lombok.extern.slf4j.Slf4j;

/**
	 * Thread-safe implementation of CollectionExecutor.
	 */
@Slf4j
public class CollectionExecutorImpl implements CollectionExecutor
{
	private final ProviderRegistry providerRegistry;
	private final CollectionRunDao collectionRunDao;
	private final CollectionResultDao collectionResultDao;
	private final CollectedEntityDao collectedEntityDao;

	public CollectionExecutorImpl(ProviderRegistry providerRegistry,
		CollectionRunDao collectionRunDao,
		CollectionResultDao collectionResultDao,
		CollectedEntityDao collectedEntityDao)
	{
		this.providerRegistry = providerRegistry;
		this.collectionRunDao = collectionRunDao;
		this.collectionResultDao = collectionResultDao;
		this.collectedEntityDao = collectedEntityDao;
	}

	public CollectionExecutorImpl(ProviderRegistry providerRegistry)
	{
		this(providerRegistry, new CollectionRunH2Dao(), new CollectionResultH2Dao(), new CollectedEntityH2Dao());
	}

	@Override
	public CollectionRun collect(CollectionPlan collectionPlan, ProviderContext providerContext) throws ExecutionException
	{
		if (log.isDebugEnabled())
		{
			log.debug("Starting collection run for with plan={} and providerContext={}",
				collectionPlan,
				providerContext);
		}

		Validate.notNull(collectionPlan, "required: collectionPlan");
		Validate.notNull(providerContext, "required: providerContext");
		
		

		List<String> executionOrder = collectionPlan.getEntityTypeExecutionOrder();
		if (executionOrder == null || executionOrder.isEmpty())
		{
			throw new ExecutionException("Collection plan has no entity types");
		}

		Optional<CloudProvider> opCloudProvider = this.providerRegistry.getLoadedCloudProvider(providerContext.getProviderId());
		if(opCloudProvider.isEmpty())
		{
			throw new IllegalStateException("could not find cloud provider");
		}
		
		Optional<CollectorRegistry> opCollectorRegistry = this.providerRegistry.getLoadedCollectorRegistry(providerContext.getProviderId());
		if(opCollectorRegistry.isEmpty())
		{
			throw new IllegalStateException("could not find collector registry");
		}
		
		//ProviderContext providerContext = collectionPlan.getProviderContext();
		CollectorContext collectorContext = collectionPlan.getCollectorContext();
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
			.providerContext(JsonSupport.toJson(providerContext))
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

		ProviderSession providerSession = opCloudProvider.get().openSession(providerContext);
		if(providerSession == null)
		{
			throw new ExecutionException("Could not open cloud provider session");
		}
		
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

			Optional<Collector> opCollector = opCollectorRegistry.get().getCollector(entityType);
			if (opCollector.isEmpty())
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
					.collectorContext(JsonSupport.toJson(collectorContext))
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
					CollectorRequestParameters requestPrams = CollectorRequestParameters.builder()
						.collectorContext(collectorContext)
						.cursorToken(cursorToken)
						.pageSize(planPageSize)
						.build();

					CollectorCursor cursor = opCollector.get().collect(requestPrams, providerSession);
					if (cursor == null)
					{
						log.debug("Collector {} returned null cursor for entityType={} in runId={}",
								opCollector.get().getClass().getName(), entityType, runId);
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
				.collectorId(opCollector.get().getClass().getName())
				.collectorContext(JsonSupport.toJson(collectorContext))
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

		CollectionRunSummary collectionRunSummary = CollectionRunSummary.builder()
			.totalCollectorCount(totalCollectorCount)
			.successfulCollectorCount(successfulCollectorCount)
			.failedCollectorCount(failedCollectorCount)
			.totalEntityCount(totalEntityCount)
			.build();

		CollectionRunFailure collectionRunFailure = null;
		if (runFailureMessage != null && !runFailureMessage.isBlank())
		{
			collectionRunFailure = CollectionRunFailure.builder()
				.message(runFailureMessage)
				.exceptionClassName(runFailureExceptionClass)
				.build();
		}

		List<CollectionResult> results = new ArrayList<>();
		for (CollectionResultEntity resultEntity : collectionResultDao.findByRunId(runId))
		{
			results.add(new DaoBackedCollectionResult(resultEntity, collectedEntityDao));
		}
		
		return new DaoBackedCollectionRun(runEntity, null, collectionRunSummary, collectionRunFailure);
	}

	
	@Override
	public CollectionRun getCollectedRun(String runId) throws ExecutionException
	{
		Validate.notEmpty(runId);
		Optional<CollectionRunEntity> opCollectionRunEntity = this.collectionRunDao.findById(runId);
		if(opCollectionRunEntity.isEmpty())
		{
			return null;
		}
		
		List<CollectionResultEntity> collectionResultEntities = this.collectionResultDao.findByRunId(runId);
		Map<String, CollectionResult> collectionResultMap = null;
		if(collectionResultEntities != null)
		{
			collectionResultMap = new HashMap<String, CollectionResult>();
			CollectionResult collectionResult = null;
			for(CollectionResultEntity collectionResultEntity : collectionResultEntities)
			{
				collectionResult = new DaoBackedCollectionResult(collectionResultEntity, this.collectedEntityDao);
				collectionResultMap.put(collectionResultEntity.getEntityType(), collectionResult);
			}
		}
		
		CollectionRunSummary collectionRunSummary = CollectionRunSummary.builder()
			.totalCollectorCount(opCollectionRunEntity.get().getTotalCollectorCount())
			.successfulCollectorCount(opCollectionRunEntity.get().getSuccessfulCollectorCount())
			.failedCollectorCount(opCollectionRunEntity.get().getFailedCollectorCount())
			.totalEntityCount(opCollectionRunEntity.get().getTotalEntityCount())
			.build();

		CollectionRunFailure collectionRunFailure = null;
		if (StringUtils.isNotEmpty(opCollectionRunEntity.get().getFailureMessage()))
		{
			collectionRunFailure = CollectionRunFailure.builder()
				.message(opCollectionRunEntity.get().getFailureMessage())
				.exceptionClassName(opCollectionRunEntity.get().getFailureExceptionClass())
				.build();
		}
		
		return new DaoBackedCollectionRun(opCollectionRunEntity.get(), collectionResultMap, collectionRunSummary, collectionRunFailure);
	}



}
