package io.coherity.estoria.collector.engine.impl.dao;

import java.util.Collection;
import java.util.List;

public interface CollectedEntityDao
{
	void save(CollectedEntityEntity entity);
	void saveAll(Collection<CollectedEntityEntity> entities);
	List<CollectedEntityEntity> findByResultId(String resultId);
	List<CollectedEntityEntity> findByProviderIdAndEntityType(String providerId, String entityType);
	List<CollectedEntityEntity> findPageByResultId(String resultId, long offset, int limit);
	void deleteByResultId(String resultId);
}
