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

class CollectionRunH2DaoTest
{
	private CollectionRunH2Dao newDao(DataSource dataSource)
	{
		return new CollectionRunH2Dao(dataSource, () -> {
		});
	}

	private CollectionRunH2Dao newBrokenDao(DataSource dataSource)
	{
		return new CollectionRunH2Dao(dataSource, () -> {
		});
	}

	private CollectionRunEntity collectionRun(
		String runId,
		String providerId,
		String providerContext,
		String status,
		Instant runStartTime,
		Instant runEndTime,
		long totalCollectorCount,
		long successfulCollectorCount,
		long failedCollectorCount,
		long totalEntityCount,
		String failureMessage,
		String failureExceptionClass)
	{
		CollectionRunEntity entity = new CollectionRunEntity();
		entity.setRunId(runId);
		entity.setProviderId(providerId);
		entity.setProviderContext(providerContext);
		entity.setStatus(status);
		entity.setRunStartTime(runStartTime);
		entity.setRunEndTime(runEndTime);
		entity.setTotalCollectorCount(totalCollectorCount);
		entity.setSuccessfulCollectorCount(successfulCollectorCount);
		entity.setFailedCollectorCount(failedCollectorCount);
		entity.setTotalEntityCount(totalEntityCount);
		entity.setFailureMessage(failureMessage);
		entity.setFailureExceptionClass(failureExceptionClass);
		return entity;
	}

	@Nested
	class CollectionRunH2DaoConstructorTest
	{
		@Test
		void givenDataSourceAndInitializer_whenConstructed_thenInstanceIsCreatedAndInitializerIsCalled()
		{
			DataSource dataSource = mock(DataSource.class);
			boolean[] initialized = new boolean[] { false };

			CollectionRunH2Dao dao = new CollectionRunH2Dao(dataSource, () -> initialized[0] = true);

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
			PreparedStatement statement = mock(PreparedStatement.class);

			Instant start = Instant.parse("2026-04-03T20:00:00Z");
			Instant end = Instant.parse("2026-04-03T20:05:00Z");

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionRunH2Dao dao = newDao(dataSource);
			CollectionRunEntity entity = collectionRun(
				"run-1",
				"provider-1",
				"FULL",
				"SUCCESS",
				start,
				end,
				5L,
				4L,
				1L,
				250L,
				null,
				null);

			dao.save(entity);

			verify(statement).setString(1, "run-1");
			verify(statement).setString(2, "provider-1");
			verify(statement).setString(3, "FULL");
			verify(statement).setString(4, "SUCCESS");
			verify(statement).setTimestamp(5, Timestamp.from(start));
			verify(statement).setTimestamp(6, Timestamp.from(end));
			verify(statement).setLong(7, 5L);
			verify(statement).setLong(8, 4L);
			verify(statement).setLong(9, 1L);
			verify(statement).setLong(10, 250L);
			verify(statement).setString(11, null);
			verify(statement).setString(12, null);
			verify(statement).executeUpdate();
		}

		@Test
		void givenEntityWithNullRunEndTimeAndFailureFields_whenCalled_thenNullAndFailureFieldsAreBound() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			Instant start = Instant.parse("2026-04-03T20:00:00Z");

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionRunH2Dao dao = newDao(dataSource);
			CollectionRunEntity entity = collectionRun(
				"run-1",
				"provider-1",
				"INCREMENTAL",
				"FAILED",
				start,
				null,
				3L,
				1L,
				2L,
				50L,
				"failure-message",
				"java.lang.IllegalStateException");

			dao.save(entity);

			verify(statement).setTimestamp(5, Timestamp.from(start));
			verify(statement).setTimestamp(6, null);
			verify(statement).setString(11, "failure-message");
			verify(statement).setString(12, "java.lang.IllegalStateException");
			verify(statement).executeUpdate();
		}

		@Test
		void givenSQLExceptionGettingConnection_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);
			CollectionRunEntity entity = collectionRun(
				"run-1",
				"provider-1",
				"FULL",
				"SUCCESS",
				Instant.now(),
				Instant.now(),
				1L,
				1L,
				0L,
				10L,
				null,
				null);

			assertThatThrownBy(() -> dao.save(entity))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to save CollectionRunEntity");
		}

		@Test
		void givenSQLExceptionPreparingStatement_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);
			CollectionRunEntity entity = collectionRun(
				"run-1",
				"provider-1",
				"FULL",
				"SUCCESS",
				Instant.now(),
				Instant.now(),
				1L,
				1L,
				0L,
				10L,
				null,
				null);

			assertThatThrownBy(() -> dao.save(entity))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to save CollectionRunEntity");
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

			when(rs.getString("run_id")).thenReturn("run-1");
			when(rs.getString("provider_id")).thenReturn("provider-1");
			when(rs.getString("collection_scope")).thenReturn("FULL");
			when(rs.getString("status")).thenReturn("SUCCESS");
			when(rs.getTimestamp("run_start_time")).thenReturn(Timestamp.from(start));
			when(rs.getTimestamp("run_end_time")).thenReturn(Timestamp.from(end));
			when(rs.getLong("total_collector_count")).thenReturn(5L);
			when(rs.getLong("successful_collector_count")).thenReturn(4L);
			when(rs.getLong("failed_collector_count")).thenReturn(1L);
			when(rs.getLong("total_entity_count")).thenReturn(250L);
			when(rs.getString("failure_message")).thenReturn(null);
			when(rs.getString("failure_exception_class")).thenReturn(null);

			CollectionRunH2Dao dao = newDao(dataSource);

			Optional<CollectionRunEntity> found = dao.findById("run-1");

			assertThat(found).isPresent();
			assertThat(found.get().getRunId()).isEqualTo("run-1");
			assertThat(found.get().getProviderId()).isEqualTo("provider-1");
			assertThat(found.get().getStatus()).isEqualTo("SUCCESS");
			assertThat(found.get().getRunStartTime()).isEqualTo(start);
			assertThat(found.get().getRunEndTime()).isEqualTo(end);
			assertThat(found.get().getTotalCollectorCount()).isEqualTo(5L);
			assertThat(found.get().getSuccessfulCollectorCount()).isEqualTo(4L);
			assertThat(found.get().getFailedCollectorCount()).isEqualTo(1L);
			assertThat(found.get().getTotalEntityCount()).isEqualTo(250L);
			assertThat(found.get().getFailureMessage()).isNull();
			assertThat(found.get().getFailureExceptionClass()).isNull();

			verify(statement).setString(1, "run-1");
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

			when(rs.getString("run_id")).thenReturn("run-1");
			when(rs.getString("provider_id")).thenReturn("provider-1");
			when(rs.getString("collection_scope")).thenReturn("INCREMENTAL");
			when(rs.getString("status")).thenReturn("FAILED");
			when(rs.getTimestamp("run_start_time")).thenReturn(Timestamp.from(start));
			when(rs.getTimestamp("run_end_time")).thenReturn(null);
			when(rs.getLong("total_collector_count")).thenReturn(3L);
			when(rs.getLong("successful_collector_count")).thenReturn(1L);
			when(rs.getLong("failed_collector_count")).thenReturn(2L);
			when(rs.getLong("total_entity_count")).thenReturn(50L);
			when(rs.getString("failure_message")).thenReturn("failure-message");
			when(rs.getString("failure_exception_class")).thenReturn("java.lang.IllegalStateException");

			CollectionRunH2Dao dao = newDao(dataSource);

			Optional<CollectionRunEntity> found = dao.findById("run-1");

			assertThat(found).isPresent();
			assertThat(found.get().getRunEndTime()).isNull();
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

			CollectionRunH2Dao dao = newDao(dataSource);

			assertThat(dao.findById("missing")).isEmpty();
			verify(statement).setString(1, "missing");
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findById("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectionRunEntity by id");
		}
	}

	@Nested
	class FindByProviderIdTest
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

			when(rs.getString("run_id")).thenReturn("run-1", "run-2");
			when(rs.getString("provider_id")).thenReturn("provider-1", "provider-1");
			when(rs.getString("collection_scope")).thenReturn("FULL", "INCREMENTAL");
			when(rs.getString("status")).thenReturn("SUCCESS", "FAILED");
			when(rs.getTimestamp("run_start_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:00:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:00:00Z")));
			when(rs.getTimestamp("run_end_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:05:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:05:00Z")));
			when(rs.getLong("total_collector_count")).thenReturn(5L, 3L);
			when(rs.getLong("successful_collector_count")).thenReturn(5L, 1L);
			when(rs.getLong("failed_collector_count")).thenReturn(0L, 2L);
			when(rs.getLong("total_entity_count")).thenReturn(250L, 50L);
			when(rs.getString("failure_message")).thenReturn(null, "failed");
			when(rs.getString("failure_exception_class")).thenReturn(null, "java.lang.IllegalArgumentException");

			CollectionRunH2Dao dao = newDao(dataSource);

			List<CollectionRunEntity> found = dao.findByProviderId("provider-1");

			assertThat(found).hasSize(2);
			assertThat(found.get(0).getRunId()).isEqualTo("run-1");
			assertThat(found.get(1).getRunId()).isEqualTo("run-2");
			assertThat(found.get(0).getProviderId()).isEqualTo("provider-1");
			assertThat(found.get(1).getProviderId()).isEqualTo("provider-1");
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

			CollectionRunH2Dao dao = newDao(dataSource);

			assertThat(dao.findByProviderId("missing")).isEmpty();
			verify(statement).setString(1, "missing");
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findByProviderId("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectionRunEntity by providerId");
		}
	}

	@Nested
	class FindByStatusTest
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

			when(rs.getString("run_id")).thenReturn("run-1", "run-2");
			when(rs.getString("provider_id")).thenReturn("provider-1", "provider-2");
			when(rs.getString("collection_scope")).thenReturn("FULL", "FULL");
			when(rs.getString("status")).thenReturn("SUCCESS", "SUCCESS");
			when(rs.getTimestamp("run_start_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:00:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:00:00Z")));
			when(rs.getTimestamp("run_end_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:05:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:05:00Z")));
			when(rs.getLong("total_collector_count")).thenReturn(5L, 6L);
			when(rs.getLong("successful_collector_count")).thenReturn(5L, 6L);
			when(rs.getLong("failed_collector_count")).thenReturn(0L, 0L);
			when(rs.getLong("total_entity_count")).thenReturn(250L, 350L);
			when(rs.getString("failure_message")).thenReturn(null, null);
			when(rs.getString("failure_exception_class")).thenReturn(null, null);

			CollectionRunH2Dao dao = newDao(dataSource);

			List<CollectionRunEntity> found = dao.findByStatus("SUCCESS");

			assertThat(found).hasSize(2);
			assertThat(found.get(0).getRunId()).isEqualTo("run-1");
			assertThat(found.get(1).getRunId()).isEqualTo("run-2");
			assertThat(found.get(0).getStatus()).isEqualTo("SUCCESS");
			assertThat(found.get(1).getStatus()).isEqualTo("SUCCESS");

			verify(statement).setString(1, "SUCCESS");
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

			CollectionRunH2Dao dao = newDao(dataSource);

			assertThat(dao.findByStatus("missing-status")).isEmpty();
			verify(statement).setString(1, "missing-status");
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findByStatus("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectionRunEntity by status");
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

			when(rs.getString("run_id")).thenReturn("run-1", "run-2");
			when(rs.getString("provider_id")).thenReturn("provider-1", "provider-2");
			when(rs.getString("collection_scope")).thenReturn("FULL", "INCREMENTAL");
			when(rs.getString("status")).thenReturn("SUCCESS", "FAILED");
			when(rs.getTimestamp("run_start_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:00:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:00:00Z")));
			when(rs.getTimestamp("run_end_time"))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T20:05:00Z")))
				.thenReturn(Timestamp.from(Instant.parse("2026-04-03T21:05:00Z")));
			when(rs.getLong("total_collector_count")).thenReturn(5L, 3L);
			when(rs.getLong("successful_collector_count")).thenReturn(5L, 1L);
			when(rs.getLong("failed_collector_count")).thenReturn(0L, 2L);
			when(rs.getLong("total_entity_count")).thenReturn(250L, 50L);
			when(rs.getString("failure_message")).thenReturn(null, "boom");
			when(rs.getString("failure_exception_class")).thenReturn(null, "java.lang.RuntimeException");

			CollectionRunH2Dao dao = newDao(dataSource);

			List<CollectionRunEntity> found = dao.findAll();

			assertThat(found).hasSize(2);
			assertThat(found.get(0).getRunId()).isEqualTo("run-1");
			assertThat(found.get(1).getRunId()).isEqualTo("run-2");
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

			CollectionRunH2Dao dao = newDao(dataSource);

			assertThat(dao.findAll()).isEmpty();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(dao::findAll)
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find all CollectionRunEntity");
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

			Instant end = Instant.parse("2026-04-03T20:05:00Z");

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionRunH2Dao dao = newDao(dataSource);

			dao.updateStatus(
				"run-1",
				"FAILED",
				end,
				"something went wrong",
				"java.lang.IllegalStateException");

			verify(statement).setString(1, "FAILED");
			verify(statement).setTimestamp(2, Timestamp.from(end));
			verify(statement).setString(3, "something went wrong");
			verify(statement).setString(4, "java.lang.IllegalStateException");
			verify(statement).setString(5, "run-1");
			verify(statement).executeUpdate();
		}

		@Test
		void givenNullEndTime_whenCalled_thenNullTimestampIsBound() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectionRunH2Dao dao = newDao(dataSource);

			dao.updateStatus(
				"run-1",
				"FAILED",
				null,
				"failure",
				"java.lang.RuntimeException");

			verify(statement).setString(1, "FAILED");
			verify(statement).setTimestamp(2, null);
			verify(statement).setString(3, "failure");
			verify(statement).setString(4, "java.lang.RuntimeException");
			verify(statement).setString(5, "run-1");
			verify(statement).executeUpdate();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.updateStatus("anything", "FAILED", Instant.now(), "boom", "java.lang.RuntimeException"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to update CollectionRunEntity status");
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

			CollectionRunH2Dao dao = newDao(dataSource);

			dao.delete("run-1");

			verify(statement).setString(1, "run-1");
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

			CollectionRunH2Dao dao = newDao(dataSource);

			dao.delete("missing");

			verify(statement).setString(1, "missing");
			verify(statement).executeUpdate();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectionRunH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.delete("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to delete CollectionRunEntity");
		}
	}
}