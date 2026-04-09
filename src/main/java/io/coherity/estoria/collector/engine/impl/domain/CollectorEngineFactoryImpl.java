package io.coherity.estoria.collector.engine.impl.domain;

import java.util.Objects;
import java.util.function.Supplier;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;

public final class CollectorEngineFactoryImpl implements CollectorEngineFactory
{
    private static volatile CollectorEngineFactoryImpl instance;
    private static Supplier<CollectorEngine> collectorEngineSupplier = CollectorEngineFactoryImpl::createDefaultCollectorEngine;

    private final CollectorEngine collectorEngine;

    CollectorEngineFactoryImpl(CollectorEngine collectorEngine)
    {
        this.collectorEngine = Objects.requireNonNull(collectorEngine, "required: collectorEngine");
    }

    private CollectorEngineFactoryImpl()
    {
        this(Objects.requireNonNull(collectorEngineSupplier, "required: collectorEngineSupplier").get());
    }

    public static CollectorEngineFactoryImpl getInstance()
    {
        CollectorEngineFactoryImpl local = instance;
        if (local == null)
        {
            synchronized (CollectorEngineFactoryImpl.class)
            {
                local = instance;
                if (local == null)
                {
                    local = new CollectorEngineFactoryImpl();
                    instance = local;
                }
            }
        }

        return local;
    }

    @Override
    public CollectorEngine getCollectorEngine()
    {
        return collectorEngine;
    }

    static void setCollectorEngineSupplier(Supplier<CollectorEngine> supplier)
    {
        collectorEngineSupplier = Objects.requireNonNull(supplier, "required: collectorEngineSupplier");
    }

    static void reset()
    {
        instance = null;
        collectorEngineSupplier = CollectorEngineFactoryImpl::createDefaultCollectorEngine;
    }

    static CollectorEngine createDefaultCollectorEngine()
    {
        ProviderRegistry providerRegistry = new ProviderRegistryImpl();
        CollectionPlanner planner = new CollectionPlannerImpl(providerRegistry);
        CollectionExecutor executor = new CollectionExecutorImpl(providerRegistry);
        SnapshotBuilder snapshotBuilder = new ProviderSnapshotBuilderImpl();

        return new CollectorEngineImpl(
            planner,
            executor,
            snapshotBuilder,
            providerRegistry);
    }
}