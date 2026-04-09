package io.coherity.estoria.collector.engine.impl.domain;

import java.util.List;
import java.util.Optional;

import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.CollectorRegistry;

public interface ProviderRegistry
{
	List<CloudProvider> getLoadedCloudProviders();
	Optional<CloudProvider> getLoadedCloudProvider(String providerId);
	Optional<CollectorRegistry> getLoadedCollectorRegistry(String providerId);
//	Set<String> getMissingCloudProviders();
}