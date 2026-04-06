package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.coherity.estoria.collector.engine.api.CollectionPlan;
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
import io.coherity.estoria.collector.spi.CollectorRequestParameters;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import io.coherity.estoria.collector.spi.ProviderSession;

class CollectionExecutorImplTest
{
	@Nested
	class CollectTest
	{
		@Test
		void givenNullCollectionPlan_whenCollectCalled_thenThrowsNullPointerException()
		{
			CollectionExecutorImpl executor = newExecutor();

			assertThatThrownBy(() -> executor.collect(null, providerContext("aws")))
				.isInstanceOf(NullPointerException.class)
				.hasMessageContaining("required: collectionPlan");
		}

		@Test
		void givenNullProviderContext_whenCollectCalled_thenThrowsNullPointerException()
		{
			CollectionExecutorImpl executor = newExecutor();

			assertThatThrownBy(() -> executor.collect(collectionPlan("aws", List.of("vpc")), null))
				.isInstanceOf(NullPointerException.class)
				.hasMessageContaining("required: providerContext");
		}

		@Test
		void givenCollectionPlanWithNoEntityTypes_whenCollectCalled_thenThrowsExecutionException()
		{
			CollectionExecutorImpl executor = newExecutor();

			assertThatThrownBy(() -> executor.collect(collectionPlan("aws", null), providerContext("aws")))
				.isInstanceOf(ExecutionException.class)
				.hasMessageContaining("Collection plan has no entity types");
		}

		@Test
		void givenCollectionPlanWithEmptyEntityTypes_whenCollectCalled_thenThrowsExecutionException()
		{
			CollectionExecutorImpl executor = newExecutor();

			assertThatThrownBy(() -> executor.collect(collectionPlan("aws", List.of()), providerContext("aws")))
				.isInstanceOf(ExecutionException.class)
				.hasMessageContaining("Collection plan has no entity types");
		}

		@Test
		void givenProviderRegistryDoesNotContainCloudProvider_whenCollectCalled_thenThrowsIllegalStateException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.empty());

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			assertThatThrownBy(() -> executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws")))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("could not find cloud provider");
		}

		@Test
		void givenProviderRegistryDoesNotContainCollectorRegistry_whenCollectCalled_thenThrowsIllegalStateException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.empty());

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			assertThatThrownBy(() -> executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws")))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("could not find collector registry");
		}

		@Test
		void givenCloudProviderReturnsNullSession_whenCollectCalled_thenThrowsExecutionException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(null);

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			assertThatThrownBy(() -> executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws")))
				.isInstanceOf(ExecutionException.class)
				.hasMessageContaining("Could not open cloud provider session");
		}

		@Test
		void givenBlankSkippedAndUntargetedEntityTypes_whenCollectCalled_thenOnlyTargetedCollectorIsExecuted()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector collector = mock(Collector.class);
			CollectorCursor cursor = mock(CollectorCursor.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("subnet")).thenReturn(Optional.of(collector));
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());
			when(collector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class))).thenReturn(cursor);
			when(cursor.getEntities()).thenReturn(List.of());
			when(cursor.getNextCursorToken()).thenReturn(Optional.empty());

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionPlan collectionPlan = CollectionPlan.builder()
				.providerId("aws")
				.collectorContext(CollectorContext.builder().build())
				.entityTypeExecutionOrder(List.of(" ", "vpc", "subnet", "instance"))
				.skippedEntityTypes(Set.of("vpc"))
				.targetEntityTypes(Set.of("subnet"))
				.build();

			CollectionRun collectionRun = executor.collect(collectionPlan, providerContext("aws"));

			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
			assertThat(collectionRun.getCollectionRunFailure()).isNull();
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(0L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(0L);

			verify(cloudProvider, times(1)).openSession(any(ProviderContext.class));
			verify(collectorRegistry, times(1)).getCollector("subnet");
			verify(collectorRegistry, never()).getCollector("vpc");
			verify(collectorRegistry, never()).getCollector("instance");
			verify(collector, times(1)).collect(any(CollectorRequestParameters.class), any(ProviderSession.class));
			verify(collectedEntityDao, never()).saveAll(any());
			verify(collectionResultDao, times(1)).save(any(CollectionResultEntity.class));
			verify(collectionRunDao, times(2)).save(any(CollectionRunEntity.class));
		}

		@Test
		void givenMissingCollectorForEntityType_whenCollectCalled_thenFailedResultIsRecordedAndRunFails()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.empty());
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws"));

			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.FAILED);
			assertThat(collectionRun.getCollectionRunFailure()).isNotNull();
			assertThat(collectionRun.getCollectionRunFailure().getMessage())
				.contains("No collector registered for entity type 'vpc'");
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(0L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(0L);

			verify(cloudProvider, times(1)).openSession(any(ProviderContext.class));
			verify(collectionResultDao, times(1)).save(any(CollectionResultEntity.class));
			verify(collectedEntityDao, never()).saveAll(any());
			verify(collectionRunDao, times(2)).save(any(CollectionRunEntity.class));
		}

		@Test
		void givenCollectorReturnsNullCursor_whenCollectCalled_thenRunSucceedsWithZeroEntities()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector collector = mock(Collector.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.of(collector));
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());
			when(collector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class))).thenReturn(null);

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws"));

			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
			assertThat(collectionRun.getCollectionRunFailure()).isNull();
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(0L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(0L);

			verify(cloudProvider, times(1)).openSession(any(ProviderContext.class));
			verify(collectedEntityDao, never()).saveAll(any());
			verify(collectionResultDao, times(1)).save(any(CollectionResultEntity.class));
		}

		@Test
		void givenPlanPageSizeIsNull_whenCollectCalled_thenCollectorRequestUsesPageSizeAllAndProviderSession()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector collector = mock(Collector.class);
			CollectorCursor cursor = mock(CollectorCursor.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.of(collector));
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());
			when(collector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class))).thenReturn(cursor);
			when(cursor.getEntities()).thenReturn(List.of());
			when(cursor.getNextCursorToken()).thenReturn(Optional.empty());

			CollectionPlan collectionPlan = CollectionPlan.builder()
				.providerId("aws")
				.collectorContext(CollectorContext.builder().build())
				.entityTypeExecutionOrder(List.of("vpc"))
				.pageSize(null)
				.build();

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			executor.collect(collectionPlan, providerContext("aws"));

			ArgumentCaptor<CollectorRequestParameters> requestCaptor = ArgumentCaptor.forClass(CollectorRequestParameters.class);
			ArgumentCaptor<ProviderSession> sessionCaptor = ArgumentCaptor.forClass(ProviderSession.class);

			verify(collector).collect(requestCaptor.capture(), sessionCaptor.capture());

			assertThat(requestCaptor.getValue().getPageSize()).isEqualTo(CollectionPlan.PAGE_SIZE_ALL);
			assertThat(sessionCaptor.getValue()).isSameAs(providerSession);
		}

		@Test
		void givenCollectorReturnsNullEntitiesAndNullNextToken_whenCollectCalled_thenNoEntitiesArePersisted()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector collector = mock(Collector.class);
			CollectorCursor cursor = mock(CollectorCursor.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.of(collector));
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());
			when(collector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class))).thenReturn(cursor);
			when(cursor.getEntities()).thenReturn(null);
			when(cursor.getNextCursorToken()).thenReturn(null);

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws"));

			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(0L);

			verify(cloudProvider, times(1)).openSession(any(ProviderContext.class));
			verify(collectedEntityDao, never()).saveAll(any());
			verify(collectionResultDao, times(1)).save(any(CollectionResultEntity.class));
		}

		@Test
		void givenCollectorReturnsMultiplePagesOfEntities_whenCollectCalled_thenEntitiesArePersistedWithCorrectOrdinalsAndSharedProviderSession()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector collector = mock(Collector.class);
			CollectorCursor firstCursor = mock(CollectorCursor.class);
			CollectorCursor secondCursor = mock(CollectorCursor.class);

			CloudEntity firstEntity = CloudEntity.builder()
				.entityIdentifier(EntityIdentifier.builder().id("id-1").build())
				.entityType("vpc")
				.rawPayload("payload-1")
				.build();

			CloudEntity secondEntity = CloudEntity.builder()
				.entityIdentifier(EntityIdentifier.builder().id("id-2").build())
				.entityType(null)
				.rawPayload(null)
				.build();

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.of(collector));
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());

			when(collector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class)))
				.thenReturn(firstCursor)
				.thenReturn(secondCursor);

			when(firstCursor.getEntities()).thenReturn(List.of(firstEntity));
			when(firstCursor.getNextCursorToken()).thenReturn(Optional.of("cursor-1"));

			when(secondCursor.getEntities()).thenReturn(List.of(secondEntity));
			when(secondCursor.getNextCursorToken()).thenReturn(Optional.empty());

			CollectionPlan collectionPlan = CollectionPlan.builder()
				.providerId("aws")
				.collectorContext(CollectorContext.builder().build())
				.entityTypeExecutionOrder(List.of("vpc"))
				.pageSize(25)
				.build();

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.collect(collectionPlan, providerContext("aws"));

			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
			assertThat(collectionRun.getCollectionRunFailure()).isNull();
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(0L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(2L);

			ArgumentCaptor<CollectorRequestParameters> requestCaptor = ArgumentCaptor.forClass(CollectorRequestParameters.class);
			ArgumentCaptor<ProviderSession> sessionCaptor = ArgumentCaptor.forClass(ProviderSession.class);
			verify(collector, times(2)).collect(requestCaptor.capture(), sessionCaptor.capture());

			List<CollectorRequestParameters> requests = requestCaptor.getAllValues();
			assertThat(requests.get(0).getCursorToken()).isEqualTo(Optional.empty());
			assertThat(requests.get(0).getPageSize()).isEqualTo(25);
			assertThat(requests.get(1).getCursorToken()).isEqualTo(Optional.of("cursor-1"));
			assertThat(requests.get(1).getPageSize()).isEqualTo(25);

			List<ProviderSession> capturedSessions = sessionCaptor.getAllValues();
			assertThat(capturedSessions).hasSize(2);
			assertThat(capturedSessions.get(0)).isSameAs(providerSession);
			assertThat(capturedSessions.get(1)).isSameAs(providerSession);

			@SuppressWarnings("unchecked")
			ArgumentCaptor<List<CollectedEntityEntity>> batchCaptor = ArgumentCaptor.forClass(List.class);
			verify(collectedEntityDao, times(2)).saveAll(batchCaptor.capture());

			List<List<CollectedEntityEntity>> batches = batchCaptor.getAllValues();

			assertThat(batches.get(0)).hasSize(1);
			assertThat(batches.get(0).get(0).getEntityId()).isEqualTo("id-1");
			assertThat(batches.get(0).get(0).getEntityType()).isEqualTo("vpc");
			assertThat(batches.get(0).get(0).getPayloadJson()).isEqualTo("payload-1");
			assertThat(batches.get(0).get(0).getEntityOrdinal()).isEqualTo(0L);

			assertThat(batches.get(1)).hasSize(1);
			assertThat(batches.get(1).get(0).getEntityId()).isEqualTo("id-2");
			assertThat(batches.get(1).get(0).getEntityType()).isEqualTo("vpc");
			assertThat(batches.get(1).get(0).getPayloadJson()).isNull();
			assertThat(batches.get(1).get(0).getEntityOrdinal()).isEqualTo(1L);

			verify(collectionResultDao, times(1)).save(any(CollectionResultEntity.class));
			verify(collectionRunDao, times(2)).save(any(CollectionRunEntity.class));
		}

		@Test
		void givenCollectorThrowsException_whenCollectCalled_thenResultIsMarkedFailedAndRunFails()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector collector = mock(Collector.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.of(collector));
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());
			when(collector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class)))
				.thenThrow(new RuntimeException("collector failure"));

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws"));

			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.FAILED);
			assertThat(collectionRun.getCollectionRunFailure()).isNotNull();
			assertThat(collectionRun.getCollectionRunFailure().getMessage()).isEqualTo("collector failure");
			assertThat(collectionRun.getCollectionRunFailure().getExceptionClassName()).isEqualTo(RuntimeException.class.getName());
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(0L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(0L);

			verify(cloudProvider, times(1)).openSession(any(ProviderContext.class));
			verify(collectedEntityDao, never()).saveAll(any());
			verify(collectionResultDao, times(1)).save(any(CollectionResultEntity.class));
		}

		@Test
		void givenOneSuccessfulCollectorAndOneFailingCollector_whenCollectCalled_thenRunIsPartialSuccess()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector vpcCollector = mock(Collector.class);
			Collector subnetCollector = mock(Collector.class);
			CollectorCursor vpcCursor = mock(CollectorCursor.class);

			CloudEntity cloudEntity = CloudEntity.builder()
				.entityIdentifier(EntityIdentifier.builder().id("id-1").build())
				.entityType("vpc")
				.rawPayload("payload")
				.build();

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectionResultDao.findByRunId(anyString())).thenReturn(List.of());

			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.of(vpcCollector));
			when(collectorRegistry.getCollector("subnet")).thenReturn(Optional.of(subnetCollector));

			when(vpcCollector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class))).thenReturn(vpcCursor);
			when(vpcCursor.getEntities()).thenReturn(List.of(cloudEntity));
			when(vpcCursor.getNextCursorToken()).thenReturn(Optional.empty());

			when(subnetCollector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class)))
				.thenThrow(new RuntimeException("subnet failure"));

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.collect(
				collectionPlan("aws", List.of("vpc", "subnet")),
				providerContext("aws"));

			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.PARTIAL_SUCCESS);
			assertThat(collectionRun.getCollectionRunFailure()).isNotNull();
			assertThat(collectionRun.getCollectionRunFailure().getMessage()).isEqualTo("subnet failure");
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(2L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(1L);

			verify(cloudProvider, times(1)).openSession(any(ProviderContext.class));
			verify(collectedEntityDao, times(1)).saveAll(any());
			verify(collectionResultDao, times(2)).save(any(CollectionResultEntity.class));
		}

		@Test
		void givenResultDaoSaveThrowsException_whenCollectCalled_thenExceptionPropagates()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CloudProvider cloudProvider = mock(CloudProvider.class);
			ProviderSession providerSession = mock(ProviderSession.class);
			CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);
			Collector collector = mock(Collector.class);
			CollectorCursor cursor = mock(CollectorCursor.class);

			when(providerRegistry.getLoadedCloudProvider("aws")).thenReturn(Optional.of(cloudProvider));
			when(providerRegistry.getLoadedCollectorRegistry("aws")).thenReturn(Optional.of(collectorRegistry));
			when(cloudProvider.openSession(any(ProviderContext.class))).thenReturn(providerSession);
			when(collectorRegistry.getCollector("vpc")).thenReturn(Optional.of(collector));
			when(collector.collect(any(CollectorRequestParameters.class), any(ProviderSession.class))).thenReturn(cursor);
			when(cursor.getEntities()).thenReturn(List.of());
			when(cursor.getNextCursorToken()).thenReturn(Optional.empty());

			doThrow(new RuntimeException("db failure"))
				.when(collectionResultDao)
				.save(any(CollectionResultEntity.class));

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			assertThatThrownBy(() -> executor.collect(collectionPlan("aws", List.of("vpc")), providerContext("aws")))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("db failure");
		}
	}

	@Nested
	class GetCollectedRunTest
	{
		@Test
		void givenWhitespaceRunId_whenGetCollectedRunCalled_thenDaoIsQueriedAndNullIsReturned()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectionRunDao.findById(" ")).thenReturn(Optional.empty());

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.getCollectedRun(" ");

			assertThat(collectionRun).isNull();

			verify(collectionRunDao, times(1)).findById(" ");
			verify(collectionResultDao, never()).findByRunId(anyString());
		}

		@Test
		void givenNullRunId_whenGetCollectedRunCalled_thenThrowsNullPointerException()
		{
			CollectionExecutorImpl executor = newExecutor();

			assertThatThrownBy(() -> executor.getCollectedRun(null))
				.isInstanceOf(NullPointerException.class);
		}

		@Test
		void givenEmptyRunId_whenGetCollectedRunCalled_thenThrowsIllegalArgumentException()
		{
			CollectionExecutorImpl executor = newExecutor();

			assertThatThrownBy(() -> executor.getCollectedRun(""))
				.isInstanceOf(IllegalArgumentException.class);
		}

		@Test
		void givenRunDoesNotExist_whenGetCollectedRunCalled_thenNullIsReturned()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectionRunDao.findById("run-1")).thenReturn(Optional.empty());

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.getCollectedRun("run-1");

			assertThat(collectionRun).isNull();

			verify(collectionRunDao, times(1)).findById("run-1");
			verify(collectionResultDao, never()).findByRunId(anyString());
		}

		@Test
		void givenRunExistsWithNoFailureAndNullResultsList_whenGetCollectedRunCalled_thenRunIsReturned()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			CollectionRunEntity collectionRunEntity = CollectionRunEntity.builder()
				.runId("run-1")
				.providerId("aws")
				.status(CollectionRunStatus.SUCCESS.name())
				.runStartTime(Instant.now())
				.runEndTime(Instant.now())
				.totalCollectorCount(3L)
				.successfulCollectorCount(3L)
				.failedCollectorCount(0L)
				.totalEntityCount(12L)
				.failureMessage(null)
				.failureExceptionClass(null)
				.build();

			when(collectionRunDao.findById("run-1")).thenReturn(Optional.of(collectionRunEntity));
			when(collectionResultDao.findByRunId("run-1")).thenReturn(null);

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.getCollectedRun("run-1");

			assertThat(collectionRun).isNotNull();
			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(3L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(3L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(0L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(12L);
			assertThat(collectionRun.getCollectionRunFailure()).isNull();
		}

		@Test
		void givenRunExistsWithFailureAndResults_whenGetCollectedRunCalled_thenRunContainsFailureSummary()
			throws ExecutionException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectionRunDao collectionRunDao = mock(CollectionRunDao.class);
			CollectionResultDao collectionResultDao = mock(CollectionResultDao.class);
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			CollectionRunEntity collectionRunEntity = CollectionRunEntity.builder()
				.runId("run-1")
				.providerId("aws")
				.status(CollectionRunStatus.PARTIAL_SUCCESS.name())
				.runStartTime(Instant.now())
				.runEndTime(Instant.now())
				.totalCollectorCount(2L)
				.successfulCollectorCount(1L)
				.failedCollectorCount(1L)
				.totalEntityCount(5L)
				.failureMessage("first failure")
				.failureExceptionClass(RuntimeException.class.getName())
				.build();

			CollectionResultEntity firstResult = CollectionResultEntity.builder()
				.resultId("result-1")
				.runId("run-1")
				.collectorId("collector-1")
				.entityType("vpc")
				.status(CollectionRunStatus.SUCCESS.name())
				.entityCount(2L)
				.collectionStartTime(Instant.now())
				.collectionEndTime(Instant.now())
				.build();

			CollectionResultEntity secondResult = CollectionResultEntity.builder()
				.resultId("result-2")
				.runId("run-1")
				.collectorId("collector-2")
				.entityType("subnet")
				.status(CollectionRunStatus.FAILED.name())
				.entityCount(0L)
				.collectionStartTime(Instant.now())
				.collectionEndTime(Instant.now())
				.failureMessage("subnet failed")
				.failureExceptionClass(RuntimeException.class.getName())
				.build();

			when(collectionRunDao.findById("run-1")).thenReturn(Optional.of(collectionRunEntity));
			when(collectionResultDao.findByRunId("run-1")).thenReturn(List.of(firstResult, secondResult));

			CollectionExecutorImpl executor = new CollectionExecutorImpl(
				providerRegistry,
				collectionRunDao,
				collectionResultDao,
				collectedEntityDao);

			CollectionRun collectionRun = executor.getCollectedRun("run-1");

			assertThat(collectionRun).isNotNull();
			assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.PARTIAL_SUCCESS);
			assertThat(collectionRun.getCollectionRunSummary().getTotalCollectorCount()).isEqualTo(2L);
			assertThat(collectionRun.getCollectionRunSummary().getSuccessfulCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getFailedCollectorCount()).isEqualTo(1L);
			assertThat(collectionRun.getCollectionRunSummary().getTotalEntityCount()).isEqualTo(5L);
			assertThat(collectionRun.getCollectionRunFailure()).isNotNull();
			assertThat(collectionRun.getCollectionRunFailure().getMessage()).isEqualTo("first failure");
			assertThat(collectionRun.getCollectionRunFailure().getExceptionClassName())
				.isEqualTo(RuntimeException.class.getName());
		}
	}

	private CollectionExecutorImpl newExecutor()
	{
		return new CollectionExecutorImpl(
			mock(ProviderRegistry.class),
			mock(CollectionRunDao.class),
			mock(CollectionResultDao.class),
			mock(CollectedEntityDao.class));
	}

	private CollectionPlan collectionPlan(String providerId, List<String> entityTypeExecutionOrder)
	{
		return CollectionPlan.builder()
			.providerId(providerId)
			.collectorContext(CollectorContext.builder().build())
			.entityTypeExecutionOrder(entityTypeExecutionOrder)
			.build();
	}

	private ProviderContext providerContext(String providerId)
	{
		return ProviderContext.builder()
			.providerId(providerId)
			.build();
	}
}