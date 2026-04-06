package io.coherity.estoria.collector.engine.impl.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CollectionResultH2DaoTest
{
	private CollectionResultH2Dao newDao(DataSource dataSource)
	{
		return new CollectionResultH2Dao(dataSource, () -> {
		});
	}

	private CollectionResultH2Dao newBrokenDao(DataSource dataSource)
	{
		return new CollectionResultH2Dao(dataSource, () -> {
		});
	}

	private CollectionResultEntity collectionResult(
		String resultId,
		String runId,
		String collectorId,
		String entityType,
		String status,
		long entityCount,
		Instant collectionStartTime,
		Instant collectionEndTime,
		String failureMessage,
		String failureExceptionClass)
	{
		CollectionResultEntity entity = new CollectionResultEntity();
		entity.setResultId(resultId);
		entity.setRunId(runId);
		entity.setCollectorId(collectorId);
		entity.setEntityType(entityType);
		entity.setStatus(status);
		entity.setEntityCount(entityCount);
		entity.setCollectionStartTime(collectionStartTime);
		entity.setCollectionEndTime(collectionEndTime);
		entity.setFailureMessage(failureMessage);
		entity.setFailureExceptionClass(failureExceptionClass);
		return entity;
	}

	@Nested
	class CollectionResultH2DaoConstructorTest
	{
		@Test
		void givenDataSourceAndInitializer_whenConstructed_thenInstanceIsCreatedAndInitializerIsCalled()
		{
			DataSource dataSource = mock(DataSource.class);
			boolean[] initialized = new boolean[] { false };

			CollectionResultH2Dao dao = new CollectionResultH2Dao(dataSource, () -> initialized[0] = true);

			assertThat(dao).isNotNull();
			assertThat(initialized[0]).isTrue();
		}
	}

	@Nested
	class SaveTest
	{
		@Test
		void givenEntity_whenCalled_thenStatementIsPopulatedAndExecuted() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement preparedStatement = mock(PreparedStatement.class);

			Instant start = Instant.parse("2026-04-03T20:00:00Z");
			Instant end = Instant.parse("2026-04-03T20:05:00Z");

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

			CollectionResultH2Dao dao = newDao(dataSource);
			CollectionResultEntity entity = collectionResult(
				"result-1",
				"run-1",
				"collector-1",
				"vpc",
				"SUCCESS",
				25L,
				start,
				end,
				null,
				null);

			dao.save(entity);

			verify(preparedStatement).setString(1, "result-1");
			verify(preparedStatement).setString(2, "run-1");
			verify(preparedStatement).setString(3, "collector-1");
			verify(preparedStatement).setString(4, null);
			verify(preparedStatement).setString(5, "vpc");
			verify(preparedStatement).setString(6, "SUCCESS");
			verify(preparedStatement).setLong(7, 25L);
			verify(preparedStatement).setTimestamp(8, Timestamp.from(start));
			verify(preparedStatement).setTimestamp(9, Timestamp.from(end));
			verify(preparedStatement).setString(10, null);
			verify(preparedStatement).setString(11, null);
			verify(preparedStatement).executeUpdate();
		}

		@Test
		void givenEntityWithNullCollectionEndTimeAndFailureFields_whenCalled_thenNullAndFailureFieldsAreBound() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			Instant start = Instant.parse("2026-04-03T20:00:00Z");

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionResultH2Dao dao = newDao(dataSource);
			CollectionResultEntity entity = collectionResult(
				"result-1",
				"run-1",
				"collector-2",
				"subnet",
				"RUNNING",
				0L,
				start,
				null,
				"failure-message",
				"java.lang.IllegalStateException");

			dao.save(entity);

			verify(statement).setString(1, "result-1");
			verify(statement).setString(2, "run-1");
			verify(statement).setString(3, "collector-2");
			verify(statement).setString(4, null);
			verify(statement).setString(5, "subnet");
			verify(statement).setString(6, "RUNNING");
			verify(statement).setLong(7, 0L);
			verify(statement).setTimestamp(8, Timestamp.from(start));
			verify(statement).setTimestamp(9, null);
			verify(statement).setString(10, "failure-message");
			verify(statement).setString(11, "java.lang.IllegalStateException");
			verify(statement).executeUpdate();
		}
		
		@Test
		void givenSQLExceptionGettingConnection_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);
			CollectionResultEntity entity = collectionResult(
				"result-1",
				"run-1",
				"collector-1",
				"vpc",
				"SUCCESS",
				1L,
				Instant.now(),
				Instant.now(),
				null,
				null);

			assertThatThrownBy(() -> dao.save(entity))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to save CollectionResultEntity");
		}

		@Test
		void givenSQLExceptionPreparingStatement_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);
			CollectionResultEntity entity = collectionResult(
				"result-1",
				"run-1",
				"collector-1",
				"vpc",
				"SUCCESS",
				1L,
				Instant.now(),
				Instant.now(),
				null,
				null);

			assertThatThrownBy(() -> dao.save(entity))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to save CollectionResultEntity");
		}
	}

	@Nested
	class FindByIdTest
	{
		@Test
		void givenMatchingRow_whenCalled_thenMatchingEntityIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			Instant start = Instant.parse("2026-04-03T20:00:00Z");
			Instant end = Instant.parse("2026-04-03T20:05:00Z");

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true);

			when(rs.getString("result_id")).thenReturn("result-1");
			when(rs.getString("run_id")).thenReturn("run-1");
			when(rs.getString("collector_id")).thenReturn("collector-1");
			when(rs.getString("entity_type")).thenReturn("vpc");
			when(rs.getString("status")).thenReturn("SUCCESS");
			when(rs.getLong("entity_count")).thenReturn(10L);
			when(rs.getTimestamp("collection_start_time")).thenReturn(Timestamp.from(start));
			when(rs.getTimestamp("collection_end_time")).thenReturn(Timestamp.from(end));
			when(rs.getString("failure_message")).thenReturn(null);
			when(rs.getString("failure_exception_class")).thenReturn(null);

			CollectionResultH2Dao dao = newDao(dataSource);

			Optional<CollectionResultEntity> found = dao.findById("result-1");

			assertThat(found).isPresent();
			assertThat(found.get().getResultId()).isEqualTo("result-1");
			assertThat(found.get().getRunId()).isEqualTo("run-1");
			assertThat(found.get().getCollectorId()).isEqualTo("collector-1");
			assertThat(found.get().getEntityType()).isEqualTo("vpc");
			assertThat(found.get().getStatus()).isEqualTo("SUCCESS");
			assertThat(found.get().getEntityCount()).isEqualTo(10L);
			assertThat(found.get().getCollectionStartTime()).isEqualTo(start);
			assertThat(found.get().getCollectionEndTime()).isEqualTo(end);
			assertThat(found.get().getFailureMessage()).isNull();
			assertThat(found.get().getFailureExceptionClass()).isNull();

			verify(statement).setString(1, "result-1");
		}

		@Test
		void givenMatchingRowWithNullEndTime_whenCalled_thenEntityContainsNullEndTime() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			Instant start = Instant.parse("2026-04-03T20:00:00Z");

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true);

			when(rs.getString("result_id")).thenReturn("result-1");
			when(rs.getString("run_id")).thenReturn("run-1");
			when(rs.getString("collector_id")).thenReturn("collector-2");
			when(rs.getString("entity_type")).thenReturn("subnet");
			when(rs.getString("status")).thenReturn("RUNNING");
			when(rs.getLong("entity_count")).thenReturn(0L);
			when(rs.getTimestamp("collection_start_time")).thenReturn(Timestamp.from(start));
			when(rs.getTimestamp("collection_end_time")).thenReturn(null);
			when(rs.getString("failure_message")).thenReturn("failure-message");
			when(rs.getString("failure_exception_class")).thenReturn("java.lang.IllegalStateException");

			CollectionResultH2Dao dao = newDao(dataSource);

			Optional<CollectionResultEntity> found = dao.findById("result-1");

			assertThat(found).isPresent();
			assertThat(found.get().getCollectionEndTime()).isNull();
			assertThat(found.get().getFailureMessage()).isEqualTo("failure-message");
			assertThat(found.get().getFailureExceptionClass()).isEqualTo("java.lang.IllegalStateException");
		}

		@Test
		void givenNoMatchingRow_whenCalled_thenEmptyOptionalIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(false);

			CollectionResultH2Dao dao = newDao(dataSource);

			assertThat(dao.findById("missing")).isEmpty();
			verify(statement).setString(1, "missing");
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findById("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectionResultEntity by id");
		}
	}

	@Nested
	class FindByRunIdTest
	{
		@Test
		void givenMatchingRows_whenCalled_thenOnlyMatchingRowsAreReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true, true, false);

			when(rs.getString("result_id")).thenReturn("result-1", "result-2");
			when(rs.getString("run_id")).thenReturn("run-1", "run-1");
			when(rs.getString("collector_id")).thenReturn("collector-1", "collector-2");
			when(rs.getString("entity_type")).thenReturn("vpc", "subnet");
			when(rs.getString("status")).thenReturn("SUCCESS", "FAILED");
			when(rs.getLong("entity_count")).thenReturn(1L, 2L);
			when(rs.getTimestamp("collection_start_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:00:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:00:00Z")));
			when(rs.getTimestamp("collection_end_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:05:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:05:00Z")));
			when(rs.getString("failure_message")).thenReturn(null, "failed");
			when(rs.getString("failure_exception_class")).thenReturn(null, "java.lang.IllegalArgumentException");

			CollectionResultH2Dao dao = newDao(dataSource);

			List<CollectionResultEntity> found = dao.findByRunId("run-1");

			assertThat(found).hasSize(2);
			assertThat(found.get(0).getResultId()).isEqualTo("result-1");
			assertThat(found.get(1).getResultId()).isEqualTo("result-2");
			assertThat(found.get(0).getRunId()).isEqualTo("run-1");
			assertThat(found.get(1).getRunId()).isEqualTo("run-1");
		}

		@Test
		void givenNoMatchingRows_whenCalled_thenEmptyListIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(false);

			CollectionResultH2Dao dao = newDao(dataSource);

			assertThat(dao.findByRunId("missing")).isEmpty();
			verify(statement).setString(1, "missing");
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findByRunId("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectionResultEntity by runId");
		}
	}

	@Nested
	class FindByEntityTypeTest
	{
		@Test
		void givenMatchingRows_whenCalled_thenOnlyMatchingRowsAreReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true, true, false);

			when(rs.getString("result_id")).thenReturn("result-1", "result-2");
			when(rs.getString("run_id")).thenReturn("run-1", "run-2");
			when(rs.getString("collector_id")).thenReturn("collector-1", "collector-2");
			when(rs.getString("entity_type")).thenReturn("vpc", "vpc");
			when(rs.getString("status")).thenReturn("SUCCESS", "RUNNING");
			when(rs.getLong("entity_count")).thenReturn(1L, 2L);
			when(rs.getTimestamp("collection_start_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:00:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:00:00Z")));
			when(rs.getTimestamp("collection_end_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:05:00Z")))
				.thenReturn(null);
			when(rs.getString("failure_message")).thenReturn(null, null);
			when(rs.getString("failure_exception_class")).thenReturn(null, null);

			CollectionResultH2Dao dao = newDao(dataSource);

			List<CollectionResultEntity> found = dao.findByEntityType("vpc");

			assertThat(found).hasSize(2);
			assertThat(found.get(0).getResultId()).isEqualTo("result-1");
			assertThat(found.get(1).getResultId()).isEqualTo("result-2");
			assertThat(found.get(0).getEntityType()).isEqualTo("vpc");
			assertThat(found.get(1).getEntityType()).isEqualTo("vpc");

			verify(statement).setString(1, "vpc");
		}

		@Test
		void givenNoMatchingRows_whenCalled_thenEmptyListIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(false);

			CollectionResultH2Dao dao = newDao(dataSource);

			assertThat(dao.findByEntityType("missing-type")).isEmpty();
			verify(statement).setString(1, "missing-type");
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findByEntityType("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectionResultEntity by entityType");
		}
	}

	@Nested
	class FindAllTest
	{
		@Test
		void givenRowsExist_whenCalled_thenSavedRowsAreIncludedInResults() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true, true, false);

			when(rs.getString("result_id")).thenReturn("result-1", "result-2");
			when(rs.getString("run_id")).thenReturn("run-1", "run-1");
			when(rs.getString("collector_id")).thenReturn("collector-1", "collector-2");
			when(rs.getString("entity_type")).thenReturn("vpc", "subnet");
			when(rs.getString("status")).thenReturn("SUCCESS", "FAILED");
			when(rs.getLong("entity_count")).thenReturn(1L, 2L);
			when(rs.getTimestamp("collection_start_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:00:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:00:00Z")));
			when(rs.getTimestamp("collection_end_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:05:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:05:00Z")));
			when(rs.getString("failure_message")).thenReturn(null, "boom");
			when(rs.getString("failure_exception_class")).thenReturn(null, "java.lang.RuntimeException");

			CollectionResultH2Dao dao = newDao(dataSource);

			List<CollectionResultEntity> found = dao.findAll();

			assertThat(found).hasSize(2);
			assertThat(found.get(0).getResultId()).isEqualTo("result-1");
			assertThat(found.get(1).getResultId()).isEqualTo("result-2");
		}

		@Test
		void givenNoRowsExist_whenCalled_thenEmptyListIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(false);

			CollectionResultH2Dao dao = newDao(dataSource);

			assertThat(dao.findAll()).isEmpty();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(dao::findAll)
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find all CollectionResultEntity");
		}
	}

	@Nested
	class UpdateStatusTest
	{
		@Test
		void givenExistingRow_whenCalled_thenStatusAndFailureFieldsAreUpdated() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionResultH2Dao dao = newDao(dataSource);

			dao.updateStatus(
				"result-1",
				"FAILED",
				"something went wrong",
				"java.lang.IllegalStateException");

			verify(statement).setString(1, "FAILED");
			verify(statement).setString(2, "something went wrong");
			verify(statement).setString(3, "java.lang.IllegalStateException");
			verify(statement).setString(4, "result-1");
			verify(statement).executeUpdate();
		}

		@Test
		void givenMissingRow_whenCalled_thenUpdateStillExecutes() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionResultH2Dao dao = newDao(dataSource);

			dao.updateStatus(
				"missing",
				"FAILED",
				"failure",
				"java.lang.RuntimeException");

			verify(statement).setString(4, "missing");
			verify(statement).executeUpdate();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.updateStatus("anything", "FAILED", "boom", "java.lang.RuntimeException"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to update CollectionResultEntity status");
		}
	}

	@Nested
	class DeleteTest
	{
		@Test
		void givenExistingRow_whenCalled_thenDeleteIsExecuted() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionResultH2Dao dao = newDao(dataSource);

			dao.delete("result-1");

			verify(statement).setString(1, "result-1");
			verify(statement).executeUpdate();
		}

		@Test
		void givenMissingRow_whenCalled_thenDeleteStillExecutes() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionResultH2Dao dao = newDao(dataSource);

			dao.delete("missing");

			verify(statement).setString(1, "missing");
			verify(statement).executeUpdate();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionResultH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.delete("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to delete CollectionResultEntity");
		}
	}
}