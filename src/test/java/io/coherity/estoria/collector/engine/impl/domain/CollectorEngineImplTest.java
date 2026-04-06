package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.spi.Collector;

class CollectorEngineImplTest
{
	private static final String AWS = "aws";

	private CollectorEngineImpl newEngine(
		CollectionPlanner collectionPlanner,
		CollectionExecutor collectionExecutor,
		SnapshotBuilder snapshotBuilder,
		ProviderRegistry providerRegistry)
	{
		return new CollectorEngineImpl(
			collectionPlanner,
			collectionExecutor,
			snapshotBuilder,
			providerRegistry);
	}

	@Nested
	class ConstructorTest
	{
		@Test
		void givenInjectedDependencies_whenConstructed_thenGettersReturnInjectedInstances()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			assertThat(collectorEngine.getPlanner()).isSameAs(collectionPlanner);
			assertThat(collectorEngine.getExecutor()).isSameAs(collectionExecutor);
			assertThat(collectorEngine.getSnapshotBuilder()).isSameAs(snapshotBuilder);
		}
	}

	@Nested
	class GetPlannerTest
	{
		@Test
		void whenCalled_thenReturnsInjectedPlanner()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			assertThat(collectorEngine.getPlanner()).isSameAs(collectionPlanner);
		}
	}

	@Nested
	class GetExecutorTest
	{
		@Test
		void whenCalled_thenReturnsInjectedExecutor()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			assertThat(collectorEngine.getExecutor()).isSameAs(collectionExecutor);
		}
	}

	@Nested
	class GetSnapshotBuilderTest
	{
		@Test
		void whenCalled_thenReturnsInjectedSnapshotBuilder()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			assertThat(collectorEngine.getSnapshotBuilder()).isSameAs(snapshotBuilder);
		}
	}

	@Nested
	class GetLoadedCloudProvidersTest
	{
		@Test
		void givenLoadedCloudProvidersExist_whenCalled_thenReturnsAllLoadedCloudProviders()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

			when(providerRegistry.getLoadedCloudProviders()).thenReturn(List.of());

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			List<?> loadedCloudProviders = collectorEngine.getLoadedCloudProviders();

			assertThat(loadedCloudProviders).isEmpty();
			verify(providerRegistry, times(1)).getLoadedCloudProviders();
		}
	}

	@Nested
	class GetLoadedCollectorsTest
	{
		@Test
		void givenProviderRegistryContainsCollectorRegistry_whenCalled_thenReturnsLoadedCollectorsForProvider()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			Collector vpcCollector = mock(Collector.class);
			Collector subnetCollector = mock(Collector.class);

			when(providerRegistry.getLoadedCollectorRegistry(AWS)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getCollectors()).thenReturn(List.of(vpcCollector, subnetCollector));
			when(vpcCollector.getEntityType()).thenReturn("vpc");
			when(subnetCollector.getEntityType()).thenReturn("subnet");

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			List<Collector> loadedCollectors = collectorEngine.getLoadedCollectors(AWS);

			assertThat(loadedCollectors)
				.extracting(Collector::getEntityType)
				.containsExactly("vpc", "subnet");

			verify(providerRegistry, times(1)).getLoadedCollectorRegistry(AWS);
			verify(collectorRegistry, times(1)).getCollectors();
		}

		@Test
		void givenProviderRegistryDoesNotContainCollectorRegistry_whenCalled_thenReturnsEmptyList()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

			when(providerRegistry.getLoadedCollectorRegistry(AWS)).thenReturn(Optional.empty());

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			List<Collector> loadedCollectors = collectorEngine.getLoadedCollectors(AWS);

			assertThat(loadedCollectors).isEmpty();
			verify(providerRegistry, times(1)).getLoadedCollectorRegistry(AWS);
		}

		@Test
		void givenCollectorRegistryContainsNoCollectors_whenCalled_thenReturnsEmptyList()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);

			when(providerRegistry.getLoadedCollectorRegistry(AWS)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getCollectors()).thenReturn(List.of());

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			List<Collector> loadedCollectors = collectorEngine.getLoadedCollectors(AWS);

			assertThat(loadedCollectors).isEmpty();
			verify(providerRegistry, times(1)).getLoadedCollectorRegistry(AWS);
			verify(collectorRegistry, times(1)).getCollectors();
		}

		@Test
		void givenCollectorRegistryReturnsCollection_whenReturnedListIsModified_thenUnderlyingRegistryDataIsNotAffected()
		{
			CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
			CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
			SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			Collector vpcCollector = mock(Collector.class);
			Collection<Collector> registryCollectors = List.of(vpcCollector);

			when(providerRegistry.getLoadedCollectorRegistry(AWS)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getCollectors()).thenReturn(registryCollectors);

			CollectorEngineImpl collectorEngine = newEngine(
				collectionPlanner,
				collectionExecutor,
				snapshotBuilder,
				providerRegistry);

			List<Collector> firstResult = collectorEngine.getLoadedCollectors(AWS);
			firstResult.clear();

			List<Collector> secondResult = collectorEngine.getLoadedCollectors(AWS);

			assertThat(firstResult).isEmpty();
			assertThat(secondResult).hasSize(1);
			verify(providerRegistry, times(2)).getLoadedCollectorRegistry(AWS);
			verify(collectorRegistry, times(2)).getCollectors();
		}
	}
}