package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.coherity.estoria.collector.engine.api.CloudEntityPage;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.ProviderSnapshot;
import io.coherity.estoria.collector.engine.api.SnapshotBuildException;
import io.coherity.estoria.collector.spi.CloudEntity;

class ProviderSnapshotBuilderImplTest
{
	private ProviderSnapshot providerSnapshot(String providerId)
	{
		ProviderSnapshot providerSnapshot = mock(ProviderSnapshot.class);
		when(providerSnapshot.getProviderId()).thenReturn(providerId);
		return providerSnapshot;
	}

	private CollectionResult collectionResult(String entityType)
	{
		CollectionResult collectionResult = mock(CollectionResult.class);
		when(collectionResult.getEntityType()).thenReturn(entityType);
		return collectionResult;
	}

	private CloudEntityPage page(List<CloudEntity> entities, String nextCursorToken)
	{
		return CloudEntityPage.builder()
			.entities(entities)
			.nextCursorToken(nextCursorToken)
			.build();
	}

	private CloudEntity entity(Object rawPayload)
	{
		return CloudEntity.builder()
			.rawPayload(rawPayload)
			.build();
	}

	private CloudEntity entity(String entityType, Object rawPayload)
	{
		return CloudEntity.builder()
			.entityType(entityType)
			.rawPayload(rawPayload)
			.build();
	}

	private CloudEntity mockEntityWithToString(String value)
	{
		CloudEntity entity = mock(CloudEntity.class);
		when(entity.toString()).thenReturn(value);
		return entity;
	}

	@Nested
	class ProviderSnapshotBuilderImplConstructorTest
	{
		@Test
		void givenExplicitBaseDir_whenConstructed_thenInstanceIsCreated()
			throws IOException
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			assertThat(builder).isNotNull();
		}

		@Test
		void givenDefaultConstructor_whenConstructed_thenInstanceIsCreated()
		{
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl();

			assertThat(builder).isNotNull();
		}
	}

	@Nested
	class MergeSnapshotTest
	{
		@Test
		void givenNullProviderSnapshot_whenCalled_thenSnapshotBuildExceptionIsThrown()
			throws IOException
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);
			CollectionResult result = mock(CollectionResult.class);

			assertThatThrownBy(() -> builder.mergeSnapshot(null, result))
				.isInstanceOf(SnapshotBuildException.class)
				.hasMessageContaining("providerSnapshot must not be null");
		}

		@Test
		void givenNullEntitiesSnapshot_whenCalled_thenSnapshotBuildExceptionIsThrown()
			throws IOException
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);
			ProviderSnapshot snapshot = mock(ProviderSnapshot.class);

			assertThatThrownBy(() -> builder.mergeSnapshot(snapshot, null))
				.isInstanceOf(SnapshotBuildException.class);
		}

		@Test
		void givenSinglePageWithRealCloudEntities_whenCalled_thenJsonLinesFileIsWrittenAndProviderSnapshotIsReturned()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("vpc");

			CloudEntity entity1 = entity("vpc", "payload-1");
			CloudEntity entity2 = entity("vpc", "payload-2");

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(List.of(entity1, entity2), null));

			ProviderSnapshot result = builder.mergeSnapshot(providerSnapshot, collectionResult);

			assertThat(result).isSameAs(providerSnapshot);

			Path expectedFile = tempDir.resolve("aws-vpc.jsonl");
			assertThat(Files.exists(expectedFile)).isTrue();

			List<String> lines = Files.readAllLines(expectedFile, StandardCharsets.UTF_8);
			assertThat(lines).hasSize(2);
			assertThat(lines.get(0)).contains("\"providerId\":\"aws\"");
			assertThat(lines.get(0)).contains("\"entityType\":\"vpc\"");
			assertThat(lines.get(0)).contains("\"entity\":\"");
			assertThat(lines.get(1)).contains("\"entity\":\"");
		}

		@Test
		void givenMultiplePages_whenCalled_thenAllPagesAreWritten()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("subnet");

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(List.of(entity("subnet", "payload-1")), "next-1"));
			when(collectionResult.getCloudEntityPage("next-1", 500))
				.thenReturn(page(List.of(entity("subnet", "payload-2")), null));

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-subnet.jsonl");
			List<String> lines = Files.readAllLines(expectedFile, StandardCharsets.UTF_8);

			assertThat(lines).hasSize(2);
			assertThat(lines.get(0)).contains("\"providerId\":\"aws\"");
			assertThat(lines.get(0)).contains("\"entityType\":\"subnet\"");
			assertThat(lines.get(1)).contains("\"providerId\":\"aws\"");
			assertThat(lines.get(1)).contains("\"entityType\":\"subnet\"");
		}

		@Test
		void givenNullPageReturnedImmediately_whenCalled_thenFileIsCreatedButEmpty()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("instance");

			when(collectionResult.getCloudEntityPage(null, 500)).thenReturn(null);

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-instance.jsonl");
			assertThat(Files.exists(expectedFile)).isTrue();
			assertThat(Files.readAllLines(expectedFile, StandardCharsets.UTF_8)).isEmpty();
		}

		@Test
		void givenEmptyEntityPageReturnedImmediately_whenCalled_thenFileIsCreatedButEmpty()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("elb");

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(List.of(), null));

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-elb.jsonl");
			assertThat(Files.exists(expectedFile)).isTrue();
			assertThat(Files.readAllLines(expectedFile, StandardCharsets.UTF_8)).isEmpty();
		}

		@Test
		void givenPageWithNullEntities_whenCalled_thenFileIsCreatedButEmpty()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("route-table");

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(CloudEntityPage.builder().entities(null).nextCursorToken(null).build());

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-route-table.jsonl");
			assertThat(Files.exists(expectedFile)).isTrue();
			assertThat(Files.readAllLines(expectedFile, StandardCharsets.UTF_8)).isEmpty();
		}

		@Test
		void givenExistingFile_whenCalledAgain_thenLinesAreAppended()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("security-group");

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(List.of(entity("security-group", "payload-1")), null));

			builder.mergeSnapshot(providerSnapshot, collectionResult);
			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-security-group.jsonl");
			List<String> lines = Files.readAllLines(expectedFile, StandardCharsets.UTF_8);

			assertThat(lines).hasSize(2);
			assertThat(lines.get(0)).contains("\"providerId\":\"aws\"");
			assertThat(lines.get(1)).contains("\"providerId\":\"aws\"");
		}

		@Test
		void givenEntityToStringLooksLikeJsonObject_whenCalled_thenEntityIsWrittenWithoutExtraQuotes()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("vpc");

			CloudEntity jsonEntity = mockEntityWithToString("{\"id\":\"vpc-1\"}");

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(List.of(jsonEntity), null));

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-vpc.jsonl");
			List<String> lines = Files.readAllLines(expectedFile, StandardCharsets.UTF_8);

			assertThat(lines).hasSize(1);
			assertThat(lines.get(0)).contains("\"entity\":{\"id\":\"vpc-1\"}");
		}

		@Test
		void givenEntityToStringHasSpecialCharacters_whenCalled_thenEntityStringIsEscaped()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("vpc");

			String payload = "line1\nline2\t\"quoted\"\\slash";
			CloudEntity specialEntity = mockEntityWithToString(payload);

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(List.of(specialEntity), null));

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-vpc.jsonl");
			List<String> lines = Files.readAllLines(expectedFile, StandardCharsets.UTF_8);

			assertThat(lines).hasSize(1);
			assertThat(lines.get(0)).contains("\\n");
			assertThat(lines.get(0)).contains("\\t");
			assertThat(lines.get(0)).contains("\\\"quoted\\\"");
			assertThat(lines.get(0)).contains("\\\\slash");
		}

		@Test
		void givenNullProviderIdAndNullEntityType_whenCalled_thenSafeFileNameAndJsonAreUsed()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot(null);
			CollectionResult collectionResult = collectionResult(null);

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(List.of(entity("payload-1")), null));

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("-.jsonl");
			assertThat(Files.exists(expectedFile)).isTrue();

			List<String> lines = Files.readAllLines(expectedFile, StandardCharsets.UTF_8);
			assertThat(lines).hasSize(1);
			assertThat(lines.get(0)).contains("\"providerId\":\"\"");
			assertThat(lines.get(0)).contains("\"entityType\":\"\"");
		}

		@Test
		void givenNullEntity_whenCalled_thenEmptyEntityStringIsWritten()
			throws Exception
		{
			Path tempDir = Files.createTempDirectory("snapshot-builder-test");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempDir);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("vpc");

			when(collectionResult.getCloudEntityPage(null, 500))
				.thenReturn(page(Arrays.asList((CloudEntity) null), null));

			builder.mergeSnapshot(providerSnapshot, collectionResult);

			Path expectedFile = tempDir.resolve("aws-vpc.jsonl");
			List<String> lines = Files.readAllLines(expectedFile, StandardCharsets.UTF_8);

			assertThat(lines).hasSize(1);
			assertThat(lines.get(0)).contains("\"entity\":\"\"");
		}

		@Test
		void givenBaseDirIsARegularFile_whenCalled_thenSnapshotBuildExceptionWrapsIOException()
			throws Exception
		{
			Path tempFile = Files.createTempFile("snapshot-builder-file", ".tmp");
			ProviderSnapshotBuilderImpl builder = new ProviderSnapshotBuilderImpl(tempFile);

			ProviderSnapshot providerSnapshot = providerSnapshot("aws");
			CollectionResult collectionResult = collectionResult("vpc");

			assertThatThrownBy(() -> builder.mergeSnapshot(providerSnapshot, collectionResult))
				.isInstanceOf(SnapshotBuildException.class)
				.hasMessageContaining("Failed to write provider snapshot file");
		}
	}
}