package io.coherity.estoria.collector.engine.impl.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.coherity.estoria.collector.engine.api.CloudEntityPage;
import io.coherity.estoria.collector.engine.api.CollectionFailure;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionSummary;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultEntity;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.EntityIdentifier;

public class DaoCollectionResult implements CollectionResult
{
	private static final int DEFAULT_PAGE_SIZE = 100;

	private final String resultId;
	private final CollectedEntityDao collectedEntityDao;
	private final CollectionResultEntity resultEntity;

	public DaoCollectionResult(String resultId, CollectionResultDao collectionResultDao, CollectedEntityDao collectedEntityDao)
	{
		this.resultId = resultId;
		this.collectedEntityDao = collectedEntityDao;
		this.resultEntity = collectionResultDao.findById(resultId)
			.orElseThrow(() -> new IllegalArgumentException("No collection result found for id " + resultId));
	}

	@Override
	public String getEntityType()
	{
		return resultEntity.getEntityType();
	}

	@Override
	public long getEntityCount()
	{
		return resultEntity.getEntityCount();
	}

	@Override
	public Instant getCollectionStartTime()
	{
		return resultEntity.getCollectionStartTime();
	}

	@Override
	public Instant getCollectionEndTime()
	{
		return resultEntity.getCollectionEndTime();
	}

	@Override
	public boolean isSuccess()
	{
		String exceptionClass = resultEntity.getFailureExceptionClass();
		String message = resultEntity.getFailureMessage();
		return (exceptionClass == null || exceptionClass.isBlank())
			&& (message == null || message.isBlank());
	}

	@Override
	public CollectionFailure getCollectionFailure()
	{
		if (isSuccess())
		{
			return null;
		}
		return CollectionFailure.builder()
			.exceptionClassName(resultEntity.getFailureExceptionClass())
			.message(resultEntity.getFailureMessage())
			.build();
	}

	@Override
	public CollectionSummary getCollectionSummary()
	{
		// Summary metrics are not currently persisted; return null to indicate
		// that no summary information is available for this result.
		return null;
	}

	@Override
	public CloudEntityPage getCloudEntityPage(String cursorToken, Integer pageSize)
	{
		int effectivePageSize = (pageSize == null || pageSize <= 0) ? DEFAULT_PAGE_SIZE : pageSize;
		long offset = 0L;
		if (cursorToken != null && !cursorToken.isBlank())
		{
			try
			{
				offset = Long.parseLong(cursorToken);
			}
			catch (NumberFormatException ignored)
			{
				offset = 0L;
			}
		}

		List<CollectedEntityEntity> storedEntities = collectedEntityDao.findPageByResultId(resultId, offset,
			effectivePageSize);
		List<CloudEntity> cloudEntities = new ArrayList<>(storedEntities.size());

		for (CollectedEntityEntity stored : storedEntities)
		{
			EntityIdentifier identifier = null;
			if (stored.getEntityId() != null && !stored.getEntityId().isBlank())
			{
				identifier = EntityIdentifier.builder()
					.id(stored.getEntityId())
					.build();
			}

			CloudEntity cloudEntity = CloudEntity.builder()
				.entityIdentifier(identifier)
				.entityType(stored.getEntityType())
				.rawPayload(stored.getPayloadJson())
				.build();
			cloudEntities.add(cloudEntity);
		}

		long nextOffset = offset + cloudEntities.size();
		String nextCursorToken = null;
		if (!cloudEntities.isEmpty() && nextOffset < resultEntity.getEntityCount())
		{
			nextCursorToken = Long.toString(nextOffset);
		}

		return CloudEntityPage.builder()
			.entities(cloudEntities)
			.nextCursorToken(nextCursorToken)
			.build();
	}
}