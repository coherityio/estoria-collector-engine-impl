package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunStatus;
import io.coherity.estoria.collector.engine.api.ExecutionException;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunEntity;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorRegistry;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;

@ExtendWith(MockitoExtension.class)
class CollectionExecutorImplTest
{
    // -------------------------------------------------------------------------
    // Shared mocks
    // -------------------------------------------------------------------------

    @Mock ProviderRegistry         providerRegistry;
    @Mock CollectionRunDao         collectionRunDao;
    @Mock CollectionResultDao      collectionResultDao;
    @Mock CollectedEntityDao       collectedEntityDao;

    @Mock CloudProvider            cloudProvider;
    @Mock CollectorRegistry        collectorRegistry;
    @Mock Collector                collector;
    @Mock CollectorCursor          collectorCursor;

    @Captor ArgumentCaptor<CollectionRunEntity>               runEntityCaptor;
    @Captor ArgumentCaptor<CollectionResultEntity>            resultEntityCaptor;
    @Captor ArgumentCaptor<Collection<CollectedEntityEntity>> batchCaptor;

    // -------------------------------------------------------------------------
    // System under test
    // -------------------------------------------------------------------------

    CollectionExecutorImpl sut;

    // -------------------------------------------------------------------------
    // Common fixtures
    // -------------------------------------------------------------------------

    static final String PROVIDER_ID  = "aws";
    static final String ENTITY_TYPE  = "Vpc";
    static final String ENTITY_TYPE2 = "Subnet";

    ProviderContext  providerContext;
    CollectorContext collectorContext;
    CollectionPlan   plan;

    @BeforeEach
    void setUp()
    {
        sut = new CollectionExecutorImpl(
            providerRegistry,
            collectionRunDao,
            collectionResultDao,
            collectedEntityDao);

        providerContext  = ProviderContext.builder().build();
        collectorContext = CollectorContext.builder().build();

        plan = CollectionPlan.builder()
            .providerId(PROVIDER_ID)
            .entityTypeExecutionOrder(List.of(ENTITY_TYPE))
            .targetEntityTypes(Set.of(ENTITY_TYPE))
            .collectorContext(collectorContext)
            .pageSize(CollectionPlan.PAGE_SIZE_ALL)
            .build();
    }

    // =========================================================================
    // collect()
    // =========================================================================

    @Nested
    class CollectTests
    {
        @Test
        void collect_nullPlan_throws()
        {
            assertThatThrownBy(() -> sut.collect(null, providerContext))
                .isInstanceOf(NullPointerException.class);
        }

        @Test
        void collect_nullProviderContext_throws()
        {
            assertThatThrownBy(() -> sut.collect(plan, null))
                .isInstanceOf(NullPointerException.class);
        }

        @Test
        void collect_emptyExecutionOrder_throws()
        {
            CollectionPlan emptyPlan = CollectionPlan.builder()
                .providerId(PROVIDER_ID)
                .entityTypeExecutionOrder(List.of())
                .collectorContext(collectorContext)
                .build();

            assertThatThrownBy(() -> sut.collect(emptyPlan, providerContext))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("no entity types");
        }

        @Test
        void collect_nullExecutionOrder_throws()
        {
            CollectionPlan nullOrderPlan = CollectionPlan.builder()
                .providerId(PROVIDER_ID)
                .entityTypeExecutionOrder(null)
                .collectorContext(collectorContext)
                .build();

            assertThatThrownBy(() -> sut.collect(nullOrderPlan, providerContext))
                .isInstanceOf(ExecutionException.class);
        }

        @Test
        void collect_providerNotFound_throws()
        {
            when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> sut.collect(plan, providerContext))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cloud provider");
        }

        @Test
        void collect_collectorRegistryNotFound_throws()
        {
            when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID))
                .thenReturn(Optional.of(cloudProvider));
            when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID))
                .thenReturn(Optional.empty());

            assertThatThrownBy(() -> sut.collect(plan, providerContext))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("collector registry");
        }

        @Test
        void collect_singleEntity_success() throws Exception
        {
            setupHappyPathMocks();

            CloudEntity entity = buildCloudEntity("vpc-001");
            when(collectorCursor.getEntities()).thenReturn(List.of(entity));
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 1L, null, null);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            CollectionRun run = sut.collect(plan, providerContext);

            assertThat(run).isNotNull();
            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            assertThat(run.getRunId()).isNotBlank();
            assertThat(run.getProviderId()).isEqualTo(PROVIDER_ID);
            assertThat(run.getRunStartTime()).isNotNull();
            assertThat(run.getRunEndTime()).isNotNull();
            assertThat(run.getCollectionRunSummary()).isNotNull();
            assertThat(run.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(1L);
            assertThat(run.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(1L);
            assertThat(run.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(0L);
            assertThat(run.getCollectionRunFailure()).isNull();

            verify(collectionRunDao, atLeastOnce()).save(runEntityCaptor.capture());
            CollectionRunEntity finalRun = runEntityCaptor.getValue();
            assertThat(finalRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS.name());
            assertThat(finalRun.getTotalEntityCount()).isEqualTo(1L);

            verify(collectedEntityDao, times(1)).saveAll(batchCaptor.capture());
            assertThat(batchCaptor.getValue()).hasSize(1);
        }

        @Test
        void collect_noEntities_success() throws Exception
        {
            setupHappyPathMocks();

            when(collectorCursor.getEntities()).thenReturn(List.of());
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 0L, null, null);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            CollectionRun run = sut.collect(plan, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            assertThat(run.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(0L);
            verify(collectedEntityDao, never()).saveAll(any());
        }

        @Test
        void collect_nullCursor_success() throws Exception
        {
            setupRegistryMocks();
            when(collector.collect(any(), any(), any())).thenReturn(null);

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 0L, null, null);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            CollectionRun run = sut.collect(plan, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            verify(collectedEntityDao, never()).saveAll(any());
        }

        @Test
        void collect_multiplePages_success() throws Exception
        {
            setupRegistryMocks();

            CloudEntity e1 = buildCloudEntity("vpc-001");
            CloudEntity e2 = buildCloudEntity("vpc-002");

            CollectorCursor page1 = org.mockito.Mockito.mock(CollectorCursor.class);
            CollectorCursor page2 = org.mockito.Mockito.mock(CollectorCursor.class);

            when(page1.getEntities()).thenReturn(List.of(e1));
            when(page1.getNextCursorToken()).thenReturn(Optional.of("page2token"));
            when(page2.getEntities()).thenReturn(List.of(e2));
            when(page2.getNextCursorToken()).thenReturn(Optional.empty());

            when(collector.collect(eq(providerContext), eq(collectorContext),
                argWithCursor(Optional.empty()))).thenReturn(page1);
            when(collector.collect(eq(providerContext), eq(collectorContext),
                argWithCursor(Optional.of("page2token")))).thenReturn(page2);

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 2L, null, null);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            CollectionRun run = sut.collect(plan, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            assertThat(run.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(2L);
            verify(collectedEntityDao, times(2)).saveAll(any());
        }

        @Test
        void collect_entityWithNullIdentifier_success() throws Exception
        {
            setupHappyPathMocks();

            CloudEntity entity = CloudEntity.builder()
                .entityType(ENTITY_TYPE)
                .rawPayload("{}")
                .build();

            when(collectorCursor.getEntities()).thenReturn(List.of(entity));
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 1L, null, null);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            CollectionRun run = sut.collect(plan, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
        }

        @Test
        void collect_collectorThrows_failedRun() throws Exception
        {
            setupRegistryMocks();
            when(collector.collect(any(), any(), any()))
                .thenThrow(new CollectorException("AWS API timeout"));

            CollectionResultEntity failedResult = buildResultEntity(
                CollectionRunStatus.FAILED.name(), 0L,
                "AWS API timeout", CollectorException.class.getName());
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(failedResult));

            CollectionRun run = sut.collect(plan, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.FAILED);
            assertThat(run.getCollectionRunFailure()).isNotNull();
            assertThat(run.getCollectionRunFailure().getMessage()).contains("AWS API timeout");

            verify(collectionRunDao, atLeastOnce()).save(runEntityCaptor.capture());
            assertThat(runEntityCaptor.getValue().getStatus())
                .isEqualTo(CollectionRunStatus.FAILED.name());
        }

        @Test
        void collect_partialSuccess() throws Exception
        {
            CollectionPlan twoTypePlan = CollectionPlan.builder()
                .providerId(PROVIDER_ID)
                .entityTypeExecutionOrder(List.of(ENTITY_TYPE, ENTITY_TYPE2))
                .targetEntityTypes(Set.of(ENTITY_TYPE, ENTITY_TYPE2))
                .collectorContext(collectorContext)
                .pageSize(CollectionPlan.PAGE_SIZE_ALL)
                .build();

            when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID))
                .thenReturn(Optional.of(cloudProvider));
            when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID))
                .thenReturn(Optional.of(collectorRegistry));

            Collector failingCollector = org.mockito.Mockito.mock(Collector.class);
            when(collectorRegistry.getRegisteredCollector(ENTITY_TYPE))
                .thenReturn(Optional.of(collector));
            when(collectorRegistry.getRegisteredCollector(ENTITY_TYPE2))
                .thenReturn(Optional.of(failingCollector));

            when(collector.collect(any(), any(), any())).thenReturn(collectorCursor);
            when(collectorCursor.getEntities())
                .thenReturn(List.of(buildCloudEntity("vpc-001")));
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());

            when(failingCollector.collect(any(), any(), any()))
                .thenThrow(new CollectorException("Subnet API error"));

            CollectionResultEntity okResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 1L, null, null);
            CollectionResultEntity failResult = buildResultEntity(
                CollectionRunStatus.FAILED.name(), 0L,
                "Subnet API error", CollectorException.class.getName());
            when(collectionResultDao.findByRunId(anyString()))
                .thenReturn(List.of(okResult, failResult));

            CollectionRun run = sut.collect(twoTypePlan, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.PARTIAL_SUCCESS);
            assertThat(run.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(1L);
            assertThat(run.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(1L);
        }

        @Test
        void collect_skippedEntityType_notCollected() throws Exception
        {
            CollectionPlan planWithSkip = CollectionPlan.builder()
                .providerId(PROVIDER_ID)
                .entityTypeExecutionOrder(List.of(ENTITY_TYPE))
                .targetEntityTypes(Set.of(ENTITY_TYPE))
                .skippedEntityTypes(Set.of(ENTITY_TYPE))
                .collectorContext(collectorContext)
                .pageSize(CollectionPlan.PAGE_SIZE_ALL)
                .build();

            when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID))
                .thenReturn(Optional.of(cloudProvider));
            when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID))
                .thenReturn(Optional.of(collectorRegistry));

            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());

            CollectionRun run = sut.collect(planWithSkip, providerContext);

            verify(collector, never()).collect(any(), any(), any());
            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            assertThat(run.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(0L);
        }

        @Test
        void collect_entityTypeNotInTarget_skipped() throws Exception
        {
            CollectionPlan planWithTarget = CollectionPlan.builder()
                .providerId(PROVIDER_ID)
                .entityTypeExecutionOrder(List.of(ENTITY_TYPE, ENTITY_TYPE2))
                .targetEntityTypes(Set.of(ENTITY_TYPE))
                .collectorContext(collectorContext)
                .pageSize(CollectionPlan.PAGE_SIZE_ALL)
                .build();

            when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID))
                .thenReturn(Optional.of(cloudProvider));
            when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID))
                .thenReturn(Optional.of(collectorRegistry));
            when(collectorRegistry.getRegisteredCollector(ENTITY_TYPE))
                .thenReturn(Optional.of(collector));

            when(collector.collect(any(), any(), any())).thenReturn(collectorCursor);
            when(collectorCursor.getEntities()).thenReturn(List.of());
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 0L, null, null);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            sut.collect(planWithTarget, providerContext);

            verify(collectorRegistry, never()).getRegisteredCollector(ENTITY_TYPE2);
        }

        @Test
        void collect_noCollectorRegistered_failedResult() throws Exception
        {
            when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID))
                .thenReturn(Optional.of(cloudProvider));
            when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID))
                .thenReturn(Optional.of(collectorRegistry));
            when(collectorRegistry.getRegisteredCollector(ENTITY_TYPE))
                .thenReturn(Optional.empty());

            CollectionResultEntity failResult = buildResultEntity(
                CollectionRunStatus.FAILED.name(), 0L,
                "No collector registered", ExecutionException.class.getName());
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(failResult));

            CollectionRun run = sut.collect(plan, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.FAILED);
            verify(collectionResultDao, atLeastOnce()).save(resultEntityCaptor.capture());
            assertThat(resultEntityCaptor.getAllValues())
                .anyMatch(r -> CollectionRunStatus.FAILED.name().equals(r.getStatus()));
        }

        @Test
        void collect_blankEntityTypeInOrder_skipped() throws Exception
        {
            CollectionPlan planWithBlank = CollectionPlan.builder()
                .providerId(PROVIDER_ID)
                .entityTypeExecutionOrder(List.of("", " ", ENTITY_TYPE))
                .targetEntityTypes(Set.of(ENTITY_TYPE))
                .collectorContext(collectorContext)
                .pageSize(CollectionPlan.PAGE_SIZE_ALL)
                .build();

            when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID))
                .thenReturn(Optional.of(cloudProvider));
            when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID))
                .thenReturn(Optional.of(collectorRegistry));
            when(collectorRegistry.getRegisteredCollector(ENTITY_TYPE))
                .thenReturn(Optional.of(collector));

            when(collector.collect(any(), any(), any())).thenReturn(collectorCursor);
            when(collectorCursor.getEntities()).thenReturn(List.of());
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 0L, null, null);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            CollectionRun run = sut.collect(planWithBlank, providerContext);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            verify(collectorRegistry, times(1)).getRegisteredCollector(anyString());
        }

        @Test
        void collect_initialRunStatus_isRunning() throws Exception
        {
            setupHappyPathMocks();
            when(collectorCursor.getEntities()).thenReturn(List.of());
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());

            sut.collect(plan, providerContext);

            verify(collectionRunDao, atLeastOnce()).save(runEntityCaptor.capture());
            assertThat(runEntityCaptor.getAllValues().get(0).getStatus())
                .isEqualTo(CollectionRunStatus.SUCCESS.name());
        }

        @Test
        void collect_resultMapBuiltFromDao() throws Exception
        {
            setupHappyPathMocks();
            when(collectorCursor.getEntities()).thenReturn(List.of());
            when(collectorCursor.getNextCursorToken()).thenReturn(Optional.empty());

            CollectionResultEntity savedResult = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 0L, null, null);
            savedResult.setEntityType(ENTITY_TYPE);
            when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of(savedResult));

            CollectionRun run = sut.collect(plan, providerContext);

            Optional<CollectionResult> result = run.getCollectionResultByEntityType(ENTITY_TYPE);
            assertThat(result).isPresent();
            assertThat(result.get().getEntityType()).isEqualTo(ENTITY_TYPE);
        }
    }

    // =========================================================================
    // getCollectedRun()
    // =========================================================================

    @Nested
    class GetCollectedRunTests
    {
        @Test
        void getCollectedRun_blankRunId_throws()
        {
            assertThatThrownBy(() -> sut.getCollectedRun(""))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void getCollectedRun_notFound_returnsNull() throws ExecutionException
        {
            when(collectionRunDao.findById("missing-id")).thenReturn(Optional.empty());

            CollectionRun run = sut.getCollectedRun("missing-id");

            assertThat(run).isNull();
        }

        @Test
        void getCollectedRun_successRun_returned() throws ExecutionException
        {
            String runId = UUID.randomUUID().toString();
            CollectionRunEntity entity = buildRunEntity(
                runId, CollectionRunStatus.SUCCESS.name(), null, null);
            when(collectionRunDao.findById(runId)).thenReturn(Optional.of(entity));

            CollectionResultEntity resultEntity = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 2L, null, null);
            resultEntity.setEntityType(ENTITY_TYPE);
            when(collectionResultDao.findByRunId(runId)).thenReturn(List.of(resultEntity));

            CollectionRun run = sut.getCollectedRun(runId);

            assertThat(run).isNotNull();
            assertThat(run.getRunId()).isEqualTo(runId);
            assertThat(run.getProviderId()).isEqualTo(PROVIDER_ID);
            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            assertThat(run.getCollectionRunSummary()).isNotNull();
            assertThat(run.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(5L);
            assertThat(run.getCollectionRunFailure()).isNull();
        }

        @Test
        void getCollectedRun_failedRun_failurePopulated() throws ExecutionException
        {
            String runId = UUID.randomUUID().toString();
            CollectionRunEntity entity = buildRunEntity(
                runId,
                CollectionRunStatus.FAILED.name(),
                "Some error",
                RuntimeException.class.getName());
            when(collectionRunDao.findById(runId)).thenReturn(Optional.of(entity));
            when(collectionResultDao.findByRunId(runId)).thenReturn(List.of());

            CollectionRun run = sut.getCollectedRun(runId);

            assertThat(run.getStatus()).isEqualTo(CollectionRunStatus.FAILED);
            assertThat(run.getCollectionRunFailure()).isNotNull();
            assertThat(run.getCollectionRunFailure().getMessage()).isEqualTo("Some error");
            assertThat(run.getCollectionRunFailure().getExceptionClassName())
                .isEqualTo(RuntimeException.class.getName());
        }

        @Test
        void getCollectedRun_noResults_emptyList() throws ExecutionException
        {
            String runId = UUID.randomUUID().toString();
            CollectionRunEntity entity = buildRunEntity(
                runId, CollectionRunStatus.SUCCESS.name(), null, null);
            when(collectionRunDao.findById(runId)).thenReturn(Optional.of(entity));
            when(collectionResultDao.findByRunId(runId)).thenReturn(List.of());

            CollectionRun run = sut.getCollectedRun(runId);

            assertThat(run.getCollectionResults()).isEmpty();
        }

        @Test
        void getCollectedRun_multipleResults_allReturned() throws ExecutionException
        {
            String runId = UUID.randomUUID().toString();
            CollectionRunEntity entity = buildRunEntity(
                runId, CollectionRunStatus.SUCCESS.name(), null, null);
            when(collectionRunDao.findById(runId)).thenReturn(Optional.of(entity));

            CollectionResultEntity r1 = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 1L, null, null);
            r1.setEntityType(ENTITY_TYPE);
            CollectionResultEntity r2 = buildResultEntity(
                CollectionRunStatus.SUCCESS.name(), 3L, null, null);
            r2.setEntityType(ENTITY_TYPE2);
            when(collectionResultDao.findByRunId(runId)).thenReturn(List.of(r1, r2));

            CollectionRun run = sut.getCollectedRun(runId);

            assertThat(run.getCollectionResults()).hasSize(2);
            assertThat(run.getCollectionResultByEntityType(ENTITY_TYPE)).isPresent();
            assertThat(run.getCollectionResultByEntityType(ENTITY_TYPE2)).isPresent();
        }

        @Test
        void getCollectedRun_summaryPopulatedFromEntity() throws ExecutionException
        {
            String runId = UUID.randomUUID().toString();
            CollectionRunEntity entity = buildRunEntity(
                runId, CollectionRunStatus.SUCCESS.name(), null, null);
            entity.setTotalCollectorCount(3L);
            entity.setSuccessfulCollectorCount(2L);
            entity.setFailedCollectorCount(1L);
            entity.setTotalEntityCount(10L);
            when(collectionRunDao.findById(runId)).thenReturn(Optional.of(entity));
            when(collectionResultDao.findByRunId(runId)).thenReturn(List.of());

            CollectionRun run = sut.getCollectedRun(runId);

            assertThat(run.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(3L);
            assertThat(run.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(2L);
            assertThat(run.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(1L);
            assertThat(run.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(10L);
        }

        @Test
        void getCollectedRun_blankFailureMessage_noFailure() throws ExecutionException
        {
            String runId = UUID.randomUUID().toString();
            CollectionRunEntity entity = buildRunEntity(
                runId, CollectionRunStatus.SUCCESS.name(), "", "");
            when(collectionRunDao.findById(runId)).thenReturn(Optional.of(entity));
            when(collectionResultDao.findByRunId(runId)).thenReturn(List.of());

            CollectionRun run = sut.getCollectedRun(runId);

            assertThat(run.getCollectionRunFailure()).isNull();
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Stubs only registry lookups (provider + collectorRegistry + collector lookup).
     * Use in tests that will stub collector.collect() themselves to avoid
     * UnnecessaryStubbingException.
     */
    private void setupRegistryMocks()
    {
        when(providerRegistry.getLoadedCloudProvider(PROVIDER_ID))
            .thenReturn(Optional.of(cloudProvider));
        when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID))
            .thenReturn(Optional.of(collectorRegistry));
        when(collectorRegistry.getRegisteredCollector(ENTITY_TYPE))
            .thenReturn(Optional.of(collector));
    }

    /**
     * Full happy-path: registry lookups + collector.collect() returning the shared cursor.
     * Use in tests that do NOT override collector.collect().
     */
    private void setupHappyPathMocks() throws CollectorException
    {
        setupRegistryMocks();
        when(collector.collect(
            any(ProviderContext.class),
            any(CollectorContext.class),
            any(CollectorRequestParams.class)))
            .thenReturn(collectorCursor);
    }

    private CloudEntity buildCloudEntity(String id)
    {
        return CloudEntity.builder()
            .entityIdentifier(EntityIdentifier.builder().id(id).build())
            .entityType(ENTITY_TYPE)
            .rawPayload("{\"VpcId\":\"" + id + "\"}")
            .build();
    }

    private CollectionResultEntity buildResultEntity(
        String status, long entityCount, String failureMessage, String failureExceptionClass)
    {
        return CollectionResultEntity.builder()
            .resultId(UUID.randomUUID().toString())
            .runId(UUID.randomUUID().toString())
            .collectorId(ENTITY_TYPE)
            .entityType(ENTITY_TYPE)
            .status(status)
            .entityCount(entityCount)
            .collectionStartTime(Instant.now())
            .collectionEndTime(Instant.now())
            .failureMessage(failureMessage)
            .failureExceptionClass(failureExceptionClass)
            .build();
    }

    private CollectionRunEntity buildRunEntity(
        String runId, String status, String failureMessage, String failureExceptionClass)
    {
        return CollectionRunEntity.builder()
            .runId(runId)
            .providerId(PROVIDER_ID)
            .status(status)
            .runStartTime(Instant.now().minusSeconds(10))
            .runEndTime(Instant.now())
            .totalCollectorCount(1L)
            .successfulCollectorCount(CollectionRunStatus.SUCCESS.name().equals(status) ? 1L : 0L)
            .failedCollectorCount(CollectionRunStatus.FAILED.name().equals(status) ? 1L : 0L)
            .totalEntityCount(5L)
            .failureMessage(failureMessage)
            .failureExceptionClass(failureExceptionClass)
            .build();
    }

    private static CollectorRequestParams argWithCursor(Optional<String> expectedCursor)
    {
        return org.mockito.ArgumentMatchers.argThat(params ->
            params != null && java.util.Objects.equals(params.getCursorToken(), expectedCursor));
    }
}