package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.coherity.estoria.collector.engine.api.CollectorEngine;

class CollectorEngineFactoryImplTest
{
    @AfterEach
    void tearDown()
    {
        CollectorEngineFactoryImpl.reset();
    }

    @Nested
    class ConstructorTest
    {
        @Test
        void givenCollectorEngine_whenFactoryConstructed_thenGetCollectorEngineReturnsInjectedInstance()
        {
            CollectorEngine collectorEngine = Mockito.mock(CollectorEngine.class);

            CollectorEngineFactoryImpl factory = new CollectorEngineFactoryImpl(collectorEngine);

            assertThat(factory.getCollectorEngine()).isSameAs(collectorEngine);
        }

        @Test
        void givenNullCollectorEngine_whenFactoryConstructed_thenThrowsNullPointerException()
        {
            assertThatThrownBy(() -> new CollectorEngineFactoryImpl((CollectorEngine) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("required: collectorEngine");
        }
    }

    @Nested
    class GetInstanceTest
    {
        @Test
        void givenSingletonNotInitialized_whenGetInstanceCalled_thenFactoryIsCreatedUsingSupplier()
        {
            CollectorEngine collectorEngine = Mockito.mock(CollectorEngine.class);
            AtomicInteger supplierInvocationCount = new AtomicInteger(0);

            CollectorEngineFactoryImpl.setCollectorEngineSupplier(() ->
            {
                supplierInvocationCount.incrementAndGet();
                return collectorEngine;
            });

            CollectorEngineFactoryImpl factory = CollectorEngineFactoryImpl.getInstance();

            assertThat(factory).isNotNull();
            assertThat(factory.getCollectorEngine()).isSameAs(collectorEngine);
            assertThat(supplierInvocationCount.get()).isEqualTo(1);
        }

        @Test
        void givenSingletonAlreadyInitialized_whenGetInstanceCalled_thenExistingInstanceIsReturned()
        {
            CollectorEngine collectorEngine = Mockito.mock(CollectorEngine.class);
            AtomicInteger supplierInvocationCount = new AtomicInteger(0);

            CollectorEngineFactoryImpl.setCollectorEngineSupplier(() ->
            {
                supplierInvocationCount.incrementAndGet();
                return collectorEngine;
            });

            CollectorEngineFactoryImpl first = CollectorEngineFactoryImpl.getInstance();
            CollectorEngineFactoryImpl second = CollectorEngineFactoryImpl.getInstance();

            assertThat(first).isSameAs(second);
            assertThat(first.getCollectorEngine()).isSameAs(collectorEngine);
            assertThat(second.getCollectorEngine()).isSameAs(collectorEngine);
            assertThat(supplierInvocationCount.get()).isEqualTo(1);
        }

        @Test
        void givenSupplierChangedAfterSingletonCreated_whenGetInstanceCalledAgain_thenOriginalSingletonIsRetained()
        {
            CollectorEngine firstCollectorEngine = Mockito.mock(CollectorEngine.class);
            CollectorEngine secondCollectorEngine = Mockito.mock(CollectorEngine.class);

            CollectorEngineFactoryImpl.setCollectorEngineSupplier(() -> firstCollectorEngine);
            CollectorEngineFactoryImpl first = CollectorEngineFactoryImpl.getInstance();

            CollectorEngineFactoryImpl.setCollectorEngineSupplier(() -> secondCollectorEngine);
            CollectorEngineFactoryImpl second = CollectorEngineFactoryImpl.getInstance();

            assertThat(first).isSameAs(second);
            assertThat(second.getCollectorEngine()).isSameAs(firstCollectorEngine);
            assertThat(second.getCollectorEngine()).isNotSameAs(secondCollectorEngine);
        }
    }

    @Nested
    class GetCollectorEngineTest
    {
        @Test
        void givenCollectorEngine_whenGetCollectorEngineCalled_thenInjectedCollectorEngineIsReturned()
        {
            CollectorEngine collectorEngine = Mockito.mock(CollectorEngine.class);
            CollectorEngineFactoryImpl factory = new CollectorEngineFactoryImpl(collectorEngine);

            CollectorEngine actualCollectorEngine = factory.getCollectorEngine();

            assertThat(actualCollectorEngine).isSameAs(collectorEngine);
        }
    }

    @Nested
    class SetCollectorEngineSupplierTest
    {
        @Test
        void givenNullSupplier_whenSetCollectorEngineSupplierCalled_thenThrowsNullPointerException()
        {
            assertThatThrownBy(() -> CollectorEngineFactoryImpl.setCollectorEngineSupplier(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("required: collectorEngineSupplier");
        }
    }

    @Nested
    class ResetTest
    {
        @Test
        void givenSingletonAndCustomSupplier_whenResetCalled_thenSingletonIsCleared()
        {
            CollectorEngine firstCollectorEngine = Mockito.mock(CollectorEngine.class);
            CollectorEngine secondCollectorEngine = Mockito.mock(CollectorEngine.class);

            CollectorEngineFactoryImpl.setCollectorEngineSupplier(() -> firstCollectorEngine);
            CollectorEngineFactoryImpl firstFactory = CollectorEngineFactoryImpl.getInstance();

            CollectorEngineFactoryImpl.reset();

            CollectorEngineFactoryImpl.setCollectorEngineSupplier(() -> secondCollectorEngine);
            CollectorEngineFactoryImpl secondFactory = CollectorEngineFactoryImpl.getInstance();

            assertThat(firstFactory).isNotSameAs(secondFactory);
            assertThat(firstFactory.getCollectorEngine()).isSameAs(firstCollectorEngine);
            assertThat(secondFactory.getCollectorEngine()).isSameAs(secondCollectorEngine);
        }
    }
    
    @Nested
    class CreateDefaultCollectorEngineTest
    {
        @Test
        void givenDefaultFactoryState_whenCreateDefaultCollectorEngineCalled_thenCollectorEngineImplIsCreated()
        {
            CollectorEngine collectorEngine = CollectorEngineFactoryImpl.createDefaultCollectorEngine();

            assertThat(collectorEngine).isNotNull();
            assertThat(collectorEngine).isInstanceOf(CollectorEngineImpl.class);

            CollectorEngineImpl collectorEngineImpl = (CollectorEngineImpl) collectorEngine;
            assertThat(collectorEngineImpl.getPlanner()).isNotNull();
            assertThat(collectorEngineImpl.getExecutor()).isNotNull();
            assertThat(collectorEngineImpl.getSnapshotBuilder()).isNotNull();
        }

        @Test
        void givenFactoryReset_whenCreateDefaultCollectorEngineCalledTwice_thenDistinctCollectorEnginesAreCreated()
        {
            CollectorEngine first = CollectorEngineFactoryImpl.createDefaultCollectorEngine();
            CollectorEngine second = CollectorEngineFactoryImpl.createDefaultCollectorEngine();

            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            assertThat(first).isNotSameAs(second);
            assertThat(first).isInstanceOf(CollectorEngineImpl.class);
            assertThat(second).isInstanceOf(CollectorEngineImpl.class);
        }
    }
    
}