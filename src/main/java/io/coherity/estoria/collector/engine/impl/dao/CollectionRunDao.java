package io.coherity.estoria.collector.engine.impl.dao;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface CollectionRunDao
{
	void save(CollectionRunEntity run);
	Optional<CollectionRunEntity> findById(String runId);
	List<CollectionRunEntity> findByProviderId(String providerId);
	List<CollectionRunEntity> findByStatus(String status);
	List<CollectionRunEntity> findAll();
	void updateStatus(String runId, String status, Instant endTime, String failureMessage,
			String failureExceptionClass);
	void delete(String runId);
}
