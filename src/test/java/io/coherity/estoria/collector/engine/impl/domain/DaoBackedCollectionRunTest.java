package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunStatus;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunEntity;
import io.coherity.estoria.collector.engine.impl.util.JsonSupport;
import io.coherity.estoria.collector.spi.ProviderContext;

class DaoBackedCollectionRunTest
{
    @Nested
    class ConstructorTest
    {
        @Test
        void givenNullCollectionRunEntity_whenConstructed_thenThrowsNullPointerException()
        {
            assertThatThrownBy(() -> new DaoBackedCollectionRun(
                null,
                Map.of(),
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("required: collectionRunEntity");
        }

        @Test
        void givenCollectionRunEntityWithEmptyRunId_whenConstructed_thenThrowsIllegalArgumentException()
        {
            CollectionRunEntity collectionRunEntity = CollectionRunEntity.builder()
                .runId("")
                .providerId("aws")
                .build();

            assertThatThrownBy(() -> new DaoBackedCollectionRun(
                collectionRunEntity,
                Map.of(),
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("required: collectionRunEntity.runId");
        }

        @Test
        void givenValidInputs_whenConstructed_thenInstanceIsCreated()
        {
            CollectionRunEntity collectionRunEntity = collectionRunEntity("run-1", "aws", "SUCCESS", "{}");
            Map<String, CollectionResult> collectionResultMap = new LinkedHashMap<>();
            CollectionRunSummary collectionRunSummary = mock(CollectionRunSummary.class);
            CollectionRunFailure collectionRunFailure = mock(CollectionRunFailure.class);

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity,
                collectionResultMap,
                collectionRunSummary,
                collectionRunFailure);

            assertThat(collectionRun).isNotNull();
            assertThat(collectionRun.getRunId()).isEqualTo("run-1");
        }
    }

    @Nested
    class GetterTest
    {
        @Test
        void givenCollectionRunEntityFields_whenGettersCalled_thenEntityValuesAreReturned()
        {
            Instant runStartTime = Instant.parse("2026-04-07T10:15:30Z");
            Instant runEndTime = Instant.parse("2026-04-07T10:20:30Z");
            ProviderContext providerContext = ProviderContext.builder()
                //.providerId("aws")
                .build();

            CollectionRunEntity collectionRunEntity = CollectionRunEntity.builder()
                .runId("run-1")
                .providerId("aws")
                .providerContext(JsonSupport.toJson(providerContext))
                .runStartTime(runStartTime)
                .runEndTime(runEndTime)
                .status("SUCCESS")
                .build();

            CollectionRunSummary collectionRunSummary = mock(CollectionRunSummary.class);
            CollectionRunFailure collectionRunFailure = mock(CollectionRunFailure.class);

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity,
                Map.of(),
                collectionRunSummary,
                collectionRunFailure);

            assertThat(collectionRun.getRunId()).isEqualTo("run-1");
            assertThat(collectionRun.getProviderId()).isEqualTo("aws");
            assertThat(collectionRun.getProviderContext()).isEqualTo(providerContext);
            assertThat(collectionRun.getRunStartTime()).isEqualTo(runStartTime);
            assertThat(collectionRun.getRunEndTime()).isEqualTo(runEndTime);
            assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
            assertThat(collectionRun.getCollectionRunSummary()).isSameAs(collectionRunSummary);
            assertThat(collectionRun.getCollectionRunFailure()).isSameAs(collectionRunFailure);
        }

        @Test
        void givenLowerCaseStatus_whenGetStatusCalled_thenStatusIsResolvedCaseInsensitively()
        {
            CollectionRunEntity collectionRunEntity = collectionRunEntity("run-1", "aws", "success", "{}");

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity,
                Map.of(),
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class));

            assertThat(collectionRun.getStatus()).isEqualTo(CollectionRunStatus.SUCCESS);
        }

        @Test
        void givenNullStatus_whenGetStatusCalled_thenNullIsReturned()
        {
            CollectionRunEntity collectionRunEntity = collectionRunEntity("run-1", "aws", null, "{}");

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity,
                Map.of(),
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class));

            assertThat(collectionRun.getStatus()).isNull();
        }

        @Test
        void givenBlankStatus_whenGetStatusCalled_thenNullIsReturned()
        {
            CollectionRunEntity collectionRunEntity = collectionRunEntity("run-1", "aws", "", "{}");

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity,
                Map.of(),
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class));

            assertThat(collectionRun.getStatus()).isNull();
        }
    }

    @Nested
    class GetCollectionResultByEntityTypeTest
    {
        @Test
        void givenMatchingEntityTypeExists_whenGetCollectionResultByEntityTypeCalled_thenResultIsReturned()
        {
            CollectionResult vpcResult = mock(CollectionResult.class);
            CollectionResult subnetResult = mock(CollectionResult.class);

            Map<String, CollectionResult> collectionResultMap = new LinkedHashMap<>();
            collectionResultMap.put("vpc", vpcResult);
            collectionResultMap.put("subnet", subnetResult);

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity("run-1", "aws", "SUCCESS", "{}"),
                collectionResultMap,
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class));

            Optional<CollectionResult> result = collectionRun.getCollectionResultByEntityType("vpc");

            assertThat(result).containsSame(vpcResult);
        }

        @Test
        void givenMatchingEntityTypeDoesNotExist_whenGetCollectionResultByEntityTypeCalled_thenEmptyOptionalIsReturned()
        {
            Map<String, CollectionResult> collectionResultMap = new LinkedHashMap<>();
            collectionResultMap.put("vpc", mock(CollectionResult.class));

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity("run-1", "aws", "SUCCESS", "{}"),
                collectionResultMap,
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class));

            Optional<CollectionResult> result = collectionRun.getCollectionResultByEntityType("subnet");

            assertThat(result).isEmpty();
        }
    }

    @Nested
    class GetCollectionResultsTest
    {
        @Test
        void givenCollectionResultsExist_whenGetCollectionResultsCalled_thenResultsAreReturnedInNewList()
        {
            CollectionResult firstResult = mock(CollectionResult.class);
            CollectionResult secondResult = mock(CollectionResult.class);

            Map<String, CollectionResult> collectionResultMap = new LinkedHashMap<>();
            collectionResultMap.put("vpc", firstResult);
            collectionResultMap.put("subnet", secondResult);

            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity("run-1", "aws", "SUCCESS", "{}"),
                collectionResultMap,
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class));

            List<CollectionResult> firstRead = collectionRun.getCollectionResults();
            firstRead.clear();

            List<CollectionResult> secondRead = collectionRun.getCollectionResults();

            assertThat(firstRead).isEmpty();
            assertThat(secondRead).containsExactly(firstResult, secondResult);
        }

        @Test
        void givenNoCollectionResultsExist_whenGetCollectionResultsCalled_thenEmptyListIsReturned()
        {
            DaoBackedCollectionRun collectionRun = new DaoBackedCollectionRun(
                collectionRunEntity("run-1", "aws", "SUCCESS", "{}"),
                Map.of(),
                mock(CollectionRunSummary.class),
                mock(CollectionRunFailure.class));

            List<CollectionResult> results = collectionRun.getCollectionResults();

            assertThat(results).isEmpty();
        }
    }

    private CollectionRunEntity collectionRunEntity(
        String runId,
        String providerId,
        String status,
        String providerContextJson)
    {
        return CollectionRunEntity.builder()
            .runId(runId)
            .providerId(providerId)
            .providerContext(providerContextJson)
            .runStartTime(Instant.parse("2026-04-07T10:00:00Z"))
            .runEndTime(Instant.parse("2026-04-07T10:05:00Z"))
            .status(status)
            .build();
    }
}