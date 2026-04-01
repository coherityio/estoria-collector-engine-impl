package io.coherity.estoria.collector.engine.impl.dao;

import java.util.List;
import java.util.Optional;

public interface CollectionResultDao
{
	void save(CollectionResultEntity result);
	Optional<CollectionResultEntity> findById(String resultId);
	List<CollectionResultEntity> findByRunId(String runId);
	List<CollectionResultEntity> findByEntityType(String entityType);
	List<CollectionResultEntity> findAll();
	void updateStatus(String resultId, String status, String failureMessage, String failureExceptionClass);
	void delete(String resultId);
}
