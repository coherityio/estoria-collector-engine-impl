package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorRegistry;
import io.coherity.estoria.collector.spi.ProviderException;
import io.coherity.estoria.collector.spi.ProviderIdentifier;
import io.coherity.estoria.collector.spi.ProviderInfo;

class CollectorEngineImplTest
{
    private static final String CLOUD_PROVIDER_SERVICE_FILE =
        "META-INF/services/io.coherity.estoria.collector.spi.CloudProvider";

    @AfterEach
    void tearDown()
    {
        CollectorEngineFactoryImpl.reset();
    }

    @Nested
    class ConstructorTest
    {
        @Test
        void givenInjectedCollaborators_whenInjectableConstructorCalled_thenGettersReturnInjectedInstances()
        {
            CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
            CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
            SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

            CollectorEngineImpl collectorEngine = new CollectorEngineImpl(
                collectionPlanner,
                collectionExecutor,
                snapshotBuilder,
                providerRegistry);

            assertThat(collectorEngine.getPlanner()).isSameAs(collectionPlanner);
            assertThat(collectorEngine.getExecutor()).isSameAs(collectionExecutor);
            assertThat(collectorEngine.getSnapshotBuilder()).isSameAs(snapshotBuilder);
        }

        @Test
        void givenInjectedCollectorEngineInFactory_whenCollectorEngineRetrieved_thenSameCollectorEngineImplIsReturned()
        {
            CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
            CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
            SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

            CollectorEngineImpl expectedCollectorEngine = new CollectorEngineImpl(
                collectionPlanner,
                collectionExecutor,
                snapshotBuilder,
                providerRegistry);

            CollectorEngineFactoryImpl collectorEngineFactory = new CollectorEngineFactoryImpl(expectedCollectorEngine);

            CollectorEngineImpl actualCollectorEngine =
                (CollectorEngineImpl) collectorEngineFactory.getCollectorEngine();

            assertThat(actualCollectorEngine).isSameAs(expectedCollectorEngine);
            assertThat(actualCollectorEngine.getPlanner()).isSameAs(collectionPlanner);
            assertThat(actualCollectorEngine.getExecutor()).isSameAs(collectionExecutor);
            assertThat(actualCollectorEngine.getSnapshotBuilder()).isSameAs(snapshotBuilder);
        }

        @Test
        void givenPlannerExecutorAndSnapshotBuilder_whenPublicConstructorCalled_thenInjectedInstancesAreRetained()
            throws Exception
        {
            CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
            CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
            SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);

            CollectorEngineImpl collectorEngine = withCloudProviderServiceHidden(
                () -> new CollectorEngineImpl(collectionPlanner, collectionExecutor, snapshotBuilder));

            assertThat(collectorEngine.getPlanner()).isSameAs(collectionPlanner);
            assertThat(collectorEngine.getExecutor()).isSameAs(collectionExecutor);
            assertThat(collectorEngine.getSnapshotBuilder()).isSameAs(snapshotBuilder);
            assertThat(collectorEngine.getLoadedCloudProviders()).isEmpty();
        }

        @Test
        void givenNoArgsConstructor_whenCalled_thenDefaultCollaboratorsAreInitialized()
            throws Exception
        {
            CollectorEngineImpl collectorEngine = withCloudProviderServiceHidden(CollectorEngineImpl::new);

            assertThat(collectorEngine.getPlanner()).isNotNull();
            assertThat(collectorEngine.getExecutor()).isNotNull();
            assertThat(collectorEngine.getSnapshotBuilder()).isNotNull();
            assertThat(collectorEngine.getLoadedCloudProviders()).isEmpty();
        }
    }

    @Nested
    class GetLoadedCloudProvidersTest
    {
        @Test
        void givenLoadedCloudProvidersInRegistry_whenGetLoadedCloudProvidersCalled_thenProvidersAreReturnedAsSet()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

            CloudProvider awsProvider = cloudProvider("aws", mock(CollectorRegistry.class));
            CloudProvider azureProvider = cloudProvider("azure", mock(CollectorRegistry.class));

            when(providerRegistry.getLoadedCloudProviders()).thenReturn(List.of(awsProvider, awsProvider, azureProvider));

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            Set<CloudProvider> loadedProviders = collectorEngine.getLoadedCloudProviders();

            assertThat(loadedProviders).containsExactlyInAnyOrder(awsProvider, azureProvider);
        }

        @Test
        void givenNoLoadedCloudProvidersInRegistry_whenGetLoadedCloudProvidersCalled_thenEmptySetIsReturned()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
            when(providerRegistry.getLoadedCloudProviders()).thenReturn(List.of());

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            Set<CloudProvider> loadedProviders = collectorEngine.getLoadedCloudProviders();

            assertThat(loadedProviders).isEmpty();
        }

        @Test
        void givenReturnedProviderSetModified_whenGetLoadedCloudProvidersCalledAgain_thenEngineStateIsUnchanged()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

            CloudProvider awsProvider = cloudProvider("aws", mock(CollectorRegistry.class));
            when(providerRegistry.getLoadedCloudProviders()).thenReturn(List.of(awsProvider));

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            Set<CloudProvider> firstResult = collectorEngine.getLoadedCloudProviders();
            firstResult.clear();

            Set<CloudProvider> secondResult = collectorEngine.getLoadedCloudProviders();

            assertThat(firstResult).isEmpty();
            assertThat(secondResult).containsExactly(awsProvider);
        }
    }

    @Nested
    class GetLoadedCloudProviderTest
    {
        @Test
        void givenProviderExists_whenGetLoadedCloudProviderCalled_thenProviderIsReturned()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

            CloudProvider awsProvider = cloudProvider("aws", mock(CollectorRegistry.class));
            when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(awsProvider));

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            Optional<CloudProvider> result = collectorEngine.getLoadedCloudProvider("aws");

            assertThat(result).containsSame(awsProvider);
        }

        @Test
        void givenProviderDoesNotExist_whenGetLoadedCloudProviderCalled_thenEmptyOptionalIsReturned()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
            when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.empty());

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            Optional<CloudProvider> result = collectorEngine.getLoadedCloudProvider("aws");

            assertThat(result).isEmpty();
        }
    }

    @Nested
    class GetRegisteredEntityTypesTest
    {
        @Test
        void givenNullProviderId_whenGetRegisteredEntityTypesCalled_thenThrowsNullPointerException()
        {
            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(mock(ProviderRegistry.class));

            assertThatThrownBy(() -> collectorEngine.getRegisteredEntityTypes(null))
                .isInstanceOf(NullPointerException.class);
        }

        @Test
        void givenEmptyProviderId_whenGetRegisteredEntityTypesCalled_thenThrowsIllegalArgumentException()
        {
            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(mock(ProviderRegistry.class));

            assertThatThrownBy(() -> collectorEngine.getRegisteredEntityTypes(""))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void givenProviderNotFound_whenGetRegisteredEntityTypesCalled_thenThrowsIllegalStateException()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
            when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.empty());

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            assertThatThrownBy(() -> collectorEngine.getRegisteredEntityTypes("aws"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("provider: aws not found");
        }

        @Test
        void givenProviderWithNullCollectorRegistry_whenGetRegisteredEntityTypesCalled_thenNullIsReturned()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);

            CloudProvider awsProvider = cloudProvider("aws", null);
            when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(awsProvider));

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            Set<String> registeredEntityTypes = collectorEngine.getRegisteredEntityTypes("aws");

            assertThat(registeredEntityTypes).isNull();
        }

        @Test
        void givenProviderWithCollectorRegistry_whenGetRegisteredEntityTypesCalled_thenRegisteredEntityTypesAreReturned()
        {
            ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
            CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);

            CloudProvider awsProvider = cloudProvider("aws", collectorRegistry);

            when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(awsProvider));
            when(collectorRegistry.getRegisteredEntityTypes()).thenReturn(Set.of("vpc", "subnet"));

            CollectorEngineImpl collectorEngine = createCollectorEngineViaFactory(providerRegistry);

            Set<String> registeredEntityTypes = collectorEngine.getRegisteredEntityTypes("aws");

            assertThat(registeredEntityTypes).containsExactlyInAnyOrder("vpc", "subnet");
        }
    }

    private CollectorEngineImpl createCollectorEngineViaFactory(ProviderRegistry providerRegistry)
    {
        CollectionPlanner collectionPlanner = mock(CollectionPlanner.class);
        CollectionExecutor collectionExecutor = mock(CollectionExecutor.class);
        SnapshotBuilder snapshotBuilder = mock(SnapshotBuilder.class);

        CollectorEngineImpl collectorEngine = new CollectorEngineImpl(
            collectionPlanner,
            collectionExecutor,
            snapshotBuilder,
            providerRegistry);

        CollectorEngineFactoryImpl.setCollectorEngineSupplier(() -> collectorEngine);

        return (CollectorEngineImpl) CollectorEngineFactoryImpl.getInstance().getCollectorEngine();
    }

    private CloudProvider cloudProvider(String providerId, CollectorRegistry collectorRegistry)
    {
        return new TestCloudProvider(providerId, collectorRegistry);
    }

    private <T> T withCloudProviderServiceHidden(ThrowingSupplier<T> supplier) throws Exception
    {
        Thread currentThread = Thread.currentThread();
        ClassLoader originalContextClassLoader = currentThread.getContextClassLoader();

        try
        {
            currentThread.setContextClassLoader(
                new CloudProviderServiceHidingClassLoader(originalContextClassLoader));
            return supplier.get();
        }
        finally
        {
            currentThread.setContextClassLoader(originalContextClassLoader);
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T>
    {
        T get() throws Exception;
    }

    private static final class CloudProviderServiceHidingClassLoader extends ClassLoader
    {
        private CloudProviderServiceHidingClassLoader(ClassLoader parent)
        {
            super(parent);
        }

        @Override
        public URL getResource(String name)
        {
            if (CLOUD_PROVIDER_SERVICE_FILE.equals(name))
            {
                return null;
            }
            return super.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException
        {
            if (CLOUD_PROVIDER_SERVICE_FILE.equals(name))
            {
                return Collections.emptyEnumeration();
            }
            return super.getResources(name);
        }
    }

    private static final class TestCloudProvider extends CloudProvider
    {
        private TestCloudProvider(String providerId, CollectorRegistry collectorRegistry)
        {
            super(
                ProviderInfo.builder()
                    .providerIdentifier(ProviderIdentifier.builder()
                        .id(providerId)
                        .version("1.0.0")
                        .build())
                    .name(providerId + "-provider")
                    .build(),
                collectorRegistry);
        }

        @Override
        public Optional<Collector> getConnectedCollector(String entityType) throws ProviderException
        {
            return Optional.empty();
        }
    }
}