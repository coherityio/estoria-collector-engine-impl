package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.coherity.estoria.collector.engine.api.CloudEntityPage;
import io.coherity.estoria.collector.engine.api.CollectionFailure;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityDao;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultEntity;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.CollectorContext;

class DaoBackedCollectionResultTest
{
	private CollectionResultEntity resultEntity(
		String resultId,
		String collectorContext,
		String collectorId,
		String entityType,
		long entityCount,
		Instant collectionStartTime,
		Instant collectionEndTime,
		String failureExceptionClass,
		String failureMessage)
	{
		return CollectionResultEntity.builder()
			.resultId(resultId)
			.collectorContext(collectorContext)
			.collectorId(collectorId)
			.entityType(entityType)
			.entityCount(entityCount)
			.collectionStartTime(collectionStartTime)
			.collectionEndTime(collectionEndTime)
			.failureExceptionClass(failureExceptionClass)
			.failureMessage(failureMessage)
			.build();
	}

	private CollectedEntityEntity storedEntity(
		String resultId,
		long entityOrdinal,
		String entityId,
		String entityType,
		String payloadJson)
	{
		return CollectedEntityEntity.builder()
			.resultId(resultId)
			.entityOrdinal(entityOrdinal)
			.entityId(entityId)
			.entityType(entityType)
			.payloadJson(payloadJson)
			.build();
	}

	@Nested
	class ConstructorTest
	{
		@Test
		void givenNullResultEntity_whenConstructed_thenThrowsNullPointerException()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			assertThatThrownBy(() -> new DaoBackedCollectionResult(null, collectedEntityDao))
				.isInstanceOf(NullPointerException.class)
				.hasMessageContaining("required: resultEntity");
		}

		@Test
		void givenNullResultEntityId_whenConstructed_thenThrowsNullPointerException()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			CollectionResultEntity resultEntity = CollectionResultEntity.builder()
				.resultId(null)
				.build();

			assertThatThrownBy(() -> new DaoBackedCollectionResult(resultEntity, collectedEntityDao))
				.isInstanceOf(NullPointerException.class)
				.hasMessageContaining("required: resultEntity.resultId");
		}

		@Test
		void givenEmptyResultEntityId_whenConstructed_thenThrowsIllegalArgumentException()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			CollectionResultEntity resultEntity = CollectionResultEntity.builder()
				.resultId("")
				.build();

			assertThatThrownBy(() -> new DaoBackedCollectionResult(resultEntity, collectedEntityDao))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("required: resultEntity.resultId");
		}

		@Test
		void givenValidInputs_whenConstructed_thenInstanceIsCreated()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 0L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			assertThat(result).isNotNull();
		}
	}

	@Nested
	class GetResultIdTest
	{
		@Test
		void whenCalled_thenReturnsResultId()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 0L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getResultId()).isEqualTo("r1");
		}
	}

	@Nested
	class GetCollectorIdTest
	{
		@Test
		void whenCalled_thenReturnsEntityType()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "subnet", 0L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getCollectorId()).isEqualTo("subnet");
		}
	}

	@Nested
	class GetCollectorContextTest
	{
		@Test
		void givenCollectorContextJson_whenCalled_thenReturnsDeserializedCollectorContext()
		{
			String collectorContextJson = "{\"attributes\":{\"region\":\"us-east-2\"}}";

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", collectorContextJson, "collector-1", "vpc", 0L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			CollectorContext collectorContext = result.getCollectorContext();

			assertThat(collectorContext).isNotNull();
			assertThat(collectorContext.getAttributes()).containsEntry("region", "us-east-2");
		}

		@Test
		void givenNullCollectorContextJson_whenCalled_thenReturnsNull()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 0L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getCollectorContext()).isNull();
		}
	}

	@Nested
	class GetEntityTypeTest
	{
		@Test
		void whenCalled_thenReturnsEntityType()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "subnet", 0L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getEntityType()).isEqualTo("subnet");
		}
	}

	@Nested
	class GetEntityCountTest
	{
		@Test
		void whenCalled_thenReturnsEntityCount()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 7L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getEntityCount()).isEqualTo(7L);
		}
	}

	@Nested
	class GetCollectionStartTimeTest
	{
		@Test
		void whenCalled_thenReturnsCollectionStartTime()
		{
			Instant collectionStartTime = Instant.parse("2026-04-03T10:15:30Z");

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 1L, collectionStartTime, Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getCollectionStartTime()).isEqualTo(collectionStartTime);
		}
	}

	@Nested
	class GetCollectionEndTimeTest
	{
		@Test
		void whenCalled_thenReturnsCollectionEndTime()
		{
			Instant collectionEndTime = Instant.parse("2026-04-03T11:15:30Z");

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 1L, Instant.now(), collectionEndTime, null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getCollectionEndTime()).isEqualTo(collectionEndTime);
		}
	}

	@Nested
	class IsSuccessTest
	{
		@Test
		void givenFailureFieldsAreNull_whenCalled_thenReturnsTrue()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 3L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.isSuccess()).isTrue();
		}

		@Test
		void givenFailureFieldsAreBlank_whenCalled_thenReturnsTrue()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 3L, Instant.now(), Instant.now(), " ", " "),
				mock(CollectedEntityDao.class));

			assertThat(result.isSuccess()).isTrue();
		}

		@Test
		void givenFailureExceptionClassExists_whenCalled_thenReturnsFalse()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 0L, Instant.now(), Instant.now(), RuntimeException.class.getName(), null),
				mock(CollectedEntityDao.class));

			assertThat(result.isSuccess()).isFalse();
		}

		@Test
		void givenFailureMessageExists_whenCalled_thenReturnsFalse()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 0L, Instant.now(), Instant.now(), null, "boom"),
				mock(CollectedEntityDao.class));

			assertThat(result.isSuccess()).isFalse();
		}
	}

	@Nested
	class GetCollectionFailureTest
	{
		@Test
		void givenSuccessfulResult_whenCalled_thenReturnsNull()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 2L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getCollectionFailure()).isNull();
		}

		@Test
		void givenFailedResult_whenCalled_thenReturnsCollectionFailure()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity(
					"r2",
					null,
					"collector-1",
					"vpc",
					0L,
					Instant.now(),
					Instant.now(),
					IllegalStateException.class.getName(),
					"boom"),
				mock(CollectedEntityDao.class));

			CollectionFailure failure = result.getCollectionFailure();

			assertThat(failure).isNotNull();
			assertThat(failure.getExceptionClassName()).isEqualTo(IllegalStateException.class.getName());
			assertThat(failure.getMessage()).isEqualTo("boom");
		}
	}

	@Nested
	class GetCollectionSummaryTest
	{
		@Test
		void whenCalled_thenReturnsNull()
		{
			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r1", null, "collector-1", "vpc", 1L, Instant.now(), Instant.now(), null, null),
				mock(CollectedEntityDao.class));

			assertThat(result.getCollectionSummary()).isNull();
		}
	}

	@Nested
	class GetCloudEntityPageTest
	{
		@Test
		void givenEntitiesStored_whenCalledWithExplicitPageSize_thenMapsEntitiesAndReturnsNextCursor()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			CollectedEntityEntity stored1 = storedEntity("r3", 0L, "id-1", "vpc", "payload-1");
			CollectedEntityEntity stored2 = storedEntity("r3", 1L, "id-2", "vpc", "payload-2");
			CollectedEntityEntity stored3 = storedEntity("r3", 2L, "id-3", "vpc", "payload-3");

			when(collectedEntityDao.findPageByResultId("r3", 0L, 2)).thenReturn(List.of(stored1, stored2));
			when(collectedEntityDao.findPageByResultId("r3", 2L, 2)).thenReturn(List.of(stored3));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r3", null, "collector-1", "vpc", 3L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page1 = result.getCloudEntityPage(null, 2);

			assertThat(page1.getEntities()).hasSize(2);
			CloudEntity firstEntity = page1.getEntities().get(0);
			assertThat(firstEntity.getEntityIdentifier()).isNotNull();
			assertThat(firstEntity.getEntityIdentifier().getId()).isEqualTo("id-1");
			assertThat(firstEntity.getEntityType()).isEqualTo("vpc");
			assertThat(firstEntity.getRawPayload()).isEqualTo("payload-1");
			assertThat(page1.getNextCursorToken()).isEqualTo("2");

			CloudEntityPage page2 = result.getCloudEntityPage("2", 2);

			assertThat(page2.getEntities()).hasSize(1);
			assertThat(page2.getEntities().get(0).getEntityIdentifier()).isNotNull();
			assertThat(page2.getEntities().get(0).getEntityIdentifier().getId()).isEqualTo("id-3");
			assertThat(page2.getNextCursorToken()).isNull();

			verify(collectedEntityDao).findPageByResultId("r3", 0L, 2);
			verify(collectedEntityDao).findPageByResultId("r3", 2L, 2);
		}

		@Test
		void givenNullPageSize_whenCalled_thenUsesDefaultPageSize()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r4", 0L, 100)).thenReturn(List.of(
				storedEntity("r4", 0L, "id-1", "subnet", "payload-1")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r4", null, "collector-1", "subnet", 1L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage(null, null);

			assertThat(page.getEntities()).hasSize(1);
			assertThat(page.getNextCursorToken()).isNull();
		}

		@Test
		void givenNonPositivePageSize_whenCalled_thenUsesDefaultPageSize()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r5", 0L, 100)).thenReturn(List.of(
				storedEntity("r5", 0L, "id-1", "instance", "payload-1")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r5", null, "collector-1", "instance", 1L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage(null, 0);

			assertThat(page.getEntities()).hasSize(1);
			assertThat(page.getNextCursorToken()).isNull();
		}

		@Test
		void givenInvalidCursorToken_whenCalled_thenFallsBackToOffsetZero()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r6", 0L, 5)).thenReturn(List.of(
				storedEntity("r6", 0L, "id-1", "vpc", "payload-1")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r6", null, "collector-1", "vpc", 1L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage("not-a-number", 5);

			assertThat(page.getEntities()).hasSize(1);
			assertThat(page.getEntities().get(0).getEntityIdentifier()).isNotNull();
			assertThat(page.getEntities().get(0).getEntityIdentifier().getId()).isEqualTo("id-1");
		}

		@Test
		void givenBlankEntityId_whenCalled_thenEntityIdentifierIsNull()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r7", 0L, 1)).thenReturn(List.of(
				storedEntity("r7", 0L, " ", "vpc", "payload-1")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r7", null, "collector-1", "vpc", 1L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage(null, 1);

			assertThat(page.getEntities()).hasSize(1);
			assertThat(page.getEntities().get(0).getEntityIdentifier()).isNull();
			assertThat(page.getEntities().get(0).getEntityType()).isEqualTo("vpc");
			assertThat(page.getEntities().get(0).getRawPayload()).isEqualTo("payload-1");
		}

		@Test
		void givenNullEntityId_whenCalled_thenEntityIdentifierIsNull()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r7b", 0L, 1)).thenReturn(List.of(
				storedEntity("r7b", 0L, null, "vpc", "payload-1")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r7b", null, "collector-1", "vpc", 1L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage(null, 1);

			assertThat(page.getEntities()).hasSize(1);
			assertThat(page.getEntities().get(0).getEntityIdentifier()).isNull();
		}

		@Test
		void givenEmptyStoredPage_whenCalled_thenReturnsEmptyPageWithNoNextCursor()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r8", 3L, 2)).thenReturn(List.of());

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r8", null, "collector-1", "vpc", 5L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage("3", 2);

			assertThat(page.getEntities()).isEmpty();
			assertThat(page.getNextCursorToken()).isNull();
		}

		@Test
		void givenBlankCursorToken_whenCalled_thenTreatsOffsetAsZero()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r9", 0L, 1)).thenReturn(List.of(
				storedEntity("r9", 0L, "id-9", "vpc", "payload-9")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r9", null, "collector-1", "vpc", 1L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage(" ", 1);

			assertThat(page.getEntities()).hasSize(1);
			assertThat(page.getEntities().get(0).getEntityIdentifier()).isNotNull();
			assertThat(page.getEntities().get(0).getEntityIdentifier().getId()).isEqualTo("id-9");
		}

		@Test
		void givenPageEndsBeforeTotalEntityCount_whenCalled_thenReturnsNextCursorToken()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r10", 1L, 2)).thenReturn(List.of(
				storedEntity("r10", 1L, "id-2", "vpc", "payload-2"),
				storedEntity("r10", 2L, "id-3", "vpc", "payload-3")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r10", null, "collector-1", "vpc", 10L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage("1", 2);

			assertThat(page.getEntities()).hasSize(2);
			assertThat(page.getNextCursorToken()).isEqualTo("3");
		}

		@Test
		void givenPageReachesTotalEntityCount_whenCalled_thenNextCursorTokenIsNull()
		{
			CollectedEntityDao collectedEntityDao = mock(CollectedEntityDao.class);

			when(collectedEntityDao.findPageByResultId("r11", 1L, 2)).thenReturn(List.of(
				storedEntity("r11", 1L, "id-2", "vpc", "payload-2"),
				storedEntity("r11", 2L, "id-3", "vpc", "payload-3")));

			DaoBackedCollectionResult result = new DaoBackedCollectionResult(
				resultEntity("r11", null, "collector-1", "vpc", 3L, Instant.now(), Instant.now(), null, null),
				collectedEntityDao);

			CloudEntityPage page = result.getCloudEntityPage("1", 2);

			assertThat(page.getEntities()).hasSize(2);
			assertThat(page.getNextCursorToken()).isNull();
		}
	}
}