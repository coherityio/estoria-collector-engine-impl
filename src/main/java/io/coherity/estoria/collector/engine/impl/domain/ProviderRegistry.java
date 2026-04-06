package io.coherity.estoria.collector.engine.impl.domain;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.spi.CloudProvider;

public interface ProviderRegistry
{
	List<CloudProvider> getLoadedCloudProviders();
	Optional<CloudProvider> getLoadedCloudProvider(String providerId);
	Optional<CollectorRegistry> getLoadedCollectorRegistry(String providerId);
	Set<String> getMissingCloudProviders();
}