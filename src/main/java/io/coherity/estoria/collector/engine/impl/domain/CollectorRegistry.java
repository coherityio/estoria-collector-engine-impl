package io.coherity.estoria.collector.engine.impl.domain;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.spi.Collector;

public interface CollectorRegistry
{
	void register(Collector collector);

	void registerAll(Collection<Collector> collectors);

	Set<String> getRegisteredEntityTypes();

	Optional<Collector> getCollector(String entityType);

	boolean hasCollector(String entityType);

	Collection<Collector> getCollectors();

	int size();

	Set<String> getDependencies(String entityType);

	Set<String> getKnownEntityTypesComplement(Set<String> entityTypes);

	Set<String> getKnownEntityTypes();

	Set<String> getUnresolvedEntityTypes();

	List<String> getExecutionOrder(String entityType) throws CircularReferenceException;
}