package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorRegistry;
import io.coherity.estoria.collector.spi.ProviderException;
import io.coherity.estoria.collector.spi.ProviderIdentifier;
import io.coherity.estoria.collector.spi.ProviderInfo;

class ProviderRegistryImplTest
{
    private static final String CLOUD_PROVIDER_SERVICE_FILE =
        "META-INF/services/io.coherity.estoria.collector.spi.CloudProvider";

    @Nested
    class ConstructorTest
    {
        @Test
        void givenNoCloudProviderServices_whenDefaultConstructorCalled_thenRegistryIsInitializedEmpty()
            throws Exception
        {
            ProviderRegistryImpl providerRegistry = withCloudProviderServiceImplementations(
                List.of(),
                ProviderRegistryImpl::new);

            assertThat(providerRegistry).isNotNull();
            assertThat(providerRegistry.getLoadedCloudProviders()).isEmpty();
            assertThat(providerRegistry.getLoadedCloudProvider("missing")).isEmpty();
            assertThat(providerRegistry.getLoadedCollectorRegistry("missing")).isEmpty();
        }

        @Test
        void givenLoadedProviderMap_whenMapConstructorCalled_thenProvidedMapIsUsed()
        {
            CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
            CloudProvider cloudProvider = cloudProvider("aws", collectorRegistry);

            Map<String, CloudProvider> loadedProviderMap = new HashMap<>();
            loadedProviderMap.put("aws", cloudProvider);

            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                loadedProviderMap,
                new HashMap<>());

            assertThat(providerRegistry.getLoadedCloudProviders()).containsExactly(cloudProvider);
            assertThat(providerRegistry.getLoadedCloudProvider("aws")).containsSame(cloudProvider);
            assertThat(providerRegistry.getLoadedCollectorRegistry("aws")).containsSame(collectorRegistry);
        }
    }

    @Nested
    class LoadProvidersTest
    {
        @Test
        void givenDiscoverableValidCloudProvider_whenLoadProvidersCalled_thenProviderIsLoaded()
            throws Exception
        {
            ExposedProviderRegistryImpl providerRegistry = new ExposedProviderRegistryImpl();

            Map<String, CloudProvider> loadedProviders = withCloudProviderServiceImplementations(
                List.of(ValidCloudProvider.class),
                providerRegistry::invokeLoadProviders);

            assertThat(loadedProviders).hasSize(1);
            assertThat(loadedProviders).containsKey("valid-provider");
            assertThat(loadedProviders.get("valid-provider")).isInstanceOf(ValidCloudProvider.class);
        }

        @Test
        void givenDiscoverableCloudProviderWithNullProviderInfo_whenLoadProvidersCalled_thenProviderIsIgnored()
            throws Exception
        {
            ExposedProviderRegistryImpl providerRegistry = new ExposedProviderRegistryImpl();

            Map<String, CloudProvider> loadedProviders = withCloudProviderServiceImplementations(
                List.of(NullProviderInfoCloudProvider.class),
                providerRegistry::invokeLoadProviders);

            assertThat(loadedProviders).isEmpty();
        }

        @Test
        void givenDiscoverableCloudProviderWithEmptyProviderId_whenLoadProvidersCalled_thenProviderIsIgnored()
            throws Exception
        {
            ExposedProviderRegistryImpl providerRegistry = new ExposedProviderRegistryImpl();

            Map<String, CloudProvider> loadedProviders = withCloudProviderServiceImplementations(
                List.of(EmptyProviderIdCloudProvider.class),
                providerRegistry::invokeLoadProviders);

            assertThat(loadedProviders).isEmpty();
        }
    }

    @Nested
    class GetLoadedCloudProvidersTest
    {
        @Test
        void givenProvidersExist_whenGetLoadedCloudProvidersCalled_thenCopyOfProvidersIsReturned()
        {
            CloudProvider awsProvider = cloudProvider("aws", mock(CollectorRegistry.class));

            Map<String, CloudProvider> loadedProviderMap = new HashMap<>();
            loadedProviderMap.put("aws", awsProvider);

            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                loadedProviderMap,
                new HashMap<>());

            List<CloudProvider> firstResult = providerRegistry.getLoadedCloudProviders();
            firstResult.clear();

            List<CloudProvider> secondResult = providerRegistry.getLoadedCloudProviders();

            assertThat(firstResult).isEmpty();
            assertThat(secondResult).containsExactly(awsProvider);
        }

        @Test
        void givenNoProvidersExist_whenGetLoadedCloudProvidersCalled_thenEmptyListIsReturned()
        {
            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                new HashMap<>(),
                new HashMap<>());

            List<CloudProvider> loadedProviders = providerRegistry.getLoadedCloudProviders();

            assertThat(loadedProviders).isEmpty();
        }
    }

    @Nested
    class GetLoadedCollectorRegistryTest
    {
        @Test
        void givenProviderExistsWithCollectorRegistry_whenGetLoadedCollectorRegistryCalled_thenRegistryIsReturned()
        {
            CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
            CloudProvider cloudProvider = cloudProvider("aws", collectorRegistry);

            Map<String, CloudProvider> loadedProviderMap = new HashMap<>();
            loadedProviderMap.put("aws", cloudProvider);

            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                loadedProviderMap,
                new HashMap<>());

            Optional<CollectorRegistry> result = providerRegistry.getLoadedCollectorRegistry("aws");

            assertThat(result).containsSame(collectorRegistry);
        }

        @Test
        void givenProviderExistsWithoutCollectorRegistry_whenGetLoadedCollectorRegistryCalled_thenEmptyOptionalIsReturned()
        {
            CloudProvider cloudProvider = cloudProvider("aws", null);

            Map<String, CloudProvider> loadedProviderMap = new HashMap<>();
            loadedProviderMap.put("aws", cloudProvider);

            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                loadedProviderMap,
                new HashMap<>());

            Optional<CollectorRegistry> result = providerRegistry.getLoadedCollectorRegistry("aws");

            assertThat(result).isEmpty();
        }

        @Test
        void givenProviderDoesNotExist_whenGetLoadedCollectorRegistryCalled_thenEmptyOptionalIsReturned()
        {
            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                new HashMap<>(),
                new HashMap<>());

            Optional<CollectorRegistry> result = providerRegistry.getLoadedCollectorRegistry("aws");

            assertThat(result).isEmpty();
        }
    }

    @Nested
    class GetLoadedCloudProviderTest
    {
        @Test
        void givenProviderExists_whenGetLoadedCloudProviderCalled_thenProviderIsReturned()
        {
            CloudProvider cloudProvider = cloudProvider("aws", mock(CollectorRegistry.class));

            Map<String, CloudProvider> loadedProviderMap = new HashMap<>();
            loadedProviderMap.put("aws", cloudProvider);

            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                loadedProviderMap,
                new HashMap<>());

            Optional<CloudProvider> result = providerRegistry.getLoadedCloudProvider("aws");

            assertThat(result).containsSame(cloudProvider);
        }

        @Test
        void givenProviderIdIsNull_whenGetLoadedCloudProviderCalled_thenEmptyOptionalIsReturned()
        {
            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                new HashMap<>(),
                new HashMap<>());

            Optional<CloudProvider> result = providerRegistry.getLoadedCloudProvider(null);

            assertThat(result).isEmpty();
        }

        @Test
        void givenProviderIdIsEmpty_whenGetLoadedCloudProviderCalled_thenEmptyOptionalIsReturned()
        {
            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                new HashMap<>(),
                new HashMap<>());

            Optional<CloudProvider> result = providerRegistry.getLoadedCloudProvider("");

            assertThat(result).isEmpty();
        }

        @Test
        void givenProviderDoesNotExist_whenGetLoadedCloudProviderCalled_thenEmptyOptionalIsReturned()
        {
            ProviderRegistryImpl providerRegistry = new ProviderRegistryImpl(
                new HashMap<>(),
                new HashMap<>());

            Optional<CloudProvider> result = providerRegistry.getLoadedCloudProvider("aws");

            assertThat(result).isEmpty();
        }
    }

    private CloudProvider cloudProvider(String providerId, CollectorRegistry collectorRegistry)
    {
        return new TestCloudProvider(providerId, collectorRegistry);
    }

    private <T> T withCloudProviderServiceImplementations(
        List<Class<? extends CloudProvider>> providerClasses,
        ThrowingSupplier<T> supplier)
        throws Exception
    {
        Path tempDirectory = Files.createTempDirectory("cloud-provider-services");
        Path serviceFile = tempDirectory.resolve(
            "META-INF/services/io.coherity.estoria.collector.spi.CloudProvider");

        Files.createDirectories(serviceFile.getParent());

        String serviceFileContents = String.join(
            System.lineSeparator(),
            providerClasses.stream().map(Class::getName).toList());

        Files.writeString(serviceFile, serviceFileContents, StandardCharsets.UTF_8);

        Thread currentThread = Thread.currentThread();
        ClassLoader originalContextClassLoader = currentThread.getContextClassLoader();

        try
        {
            currentThread.setContextClassLoader(
                new ServiceFileOverridingClassLoader(originalContextClassLoader, serviceFile.toUri().toURL()));
            return supplier.get();
        }
        finally
        {
            currentThread.setContextClassLoader(originalContextClassLoader);
            Files.deleteIfExists(serviceFile);
            Files.deleteIfExists(serviceFile.getParent());
            Files.deleteIfExists(serviceFile.getParent().getParent());
            Files.deleteIfExists(tempDirectory);
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T>
    {
        T get() throws Exception;
    }

    private static final class ServiceFileOverridingClassLoader extends ClassLoader
    {
        private final URL serviceFileUrl;

        private ServiceFileOverridingClassLoader(ClassLoader parent, URL serviceFileUrl)
        {
            super(parent);
            this.serviceFileUrl = serviceFileUrl;
        }

        @Override
        public URL getResource(String name)
        {
            if (CLOUD_PROVIDER_SERVICE_FILE.equals(name))
            {
                return serviceFileUrl;
            }
            return super.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException
        {
            if (CLOUD_PROVIDER_SERVICE_FILE.equals(name))
            {
                return Collections.enumeration(List.of(serviceFileUrl));
            }
            return super.getResources(name);
        }
    }

    private static final class ExposedProviderRegistryImpl extends ProviderRegistryImpl
    {
        private ExposedProviderRegistryImpl()
        {
            super(new HashMap<>(), new HashMap<>());
        }

        private Map<String, CloudProvider> invokeLoadProviders()
        {
            return super.loadProviders();
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

    public static final class ValidCloudProvider extends CloudProvider
    {
        public ValidCloudProvider()
        {
            super(
                ProviderInfo.builder()
                    .providerIdentifier(ProviderIdentifier.builder()
                        .id("valid-provider")
                        .version("1.0.0")
                        .build())
                    .name("valid-provider")
                    .build(),
                null);
        }

        @Override
        public Optional<Collector> getConnectedCollector(String entityType) throws ProviderException
        {
            return Optional.empty();
        }
    }

    public static final class NullProviderInfoCloudProvider extends CloudProvider
    {
        public NullProviderInfoCloudProvider()
        {
            super(null, null);
        }

        @Override
        public Optional<Collector> getConnectedCollector(String entityType) throws ProviderException
        {
            return Optional.empty();
        }
    }

    public static final class EmptyProviderIdCloudProvider extends CloudProvider
    {
        public EmptyProviderIdCloudProvider()
        {
            super(
                ProviderInfo.builder()
                    .providerIdentifier(ProviderIdentifier.builder()
                        .id("")
                        .version("1.0.0")
                        .build())
                    .name("empty-provider")
                    .build(),
                null);
        }

        @Override
        public Optional<Collector> getConnectedCollector(String entityType) throws ProviderException
        {
            return Optional.empty();
        }
    }
}