package io.coherity.estoria.collector.engine.impl.cli;

import java.time.Instant;

import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunStatus;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class CollectionRunView
{
    String runId;
    String providerId;
    ProviderContext providerContext;
    Instant runStartTime;
    Instant runEndTime;
    CollectionRunStatus status;
    CollectionRunSummary summary;
    CollectionRunFailure failure;

    public static CollectionRunView from(CollectionRun collectionRun)
    {
        return CollectionRunView.builder()
            .runId(collectionRun.getRunId())
            .providerId(collectionRun.getProviderId())
            .providerContext(collectionRun.getProviderContext())
            .runStartTime(collectionRun.getRunStartTime())
            .runEndTime(collectionRun.getRunEndTime())
            .status(collectionRun.getStatus())
            .summary(collectionRun.getCollectionRunSummary())
            .failure(collectionRun.getCollectionRunFailure())
            .build();
    }
}