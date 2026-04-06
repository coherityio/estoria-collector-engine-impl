package io.coherity.estoria.collector.engine.impl.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CollectedEntityH2DaoTest
{
	private CollectedEntityH2Dao newDao(DataSource dataSource)
	{
		return new CollectedEntityH2Dao(dataSource, () -> {
		});
	}

	private CollectedEntityH2Dao newBrokenDao(DataSource dataSource)
	{
		return new CollectedEntityH2Dao(dataSource, () -> {
		});
	}

	private CollectedEntityEntity collectedEntity(
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

	private ResultSet mockCollectedEntityRow(
		ResultSet rs,
		String resultId,
		long entityOrdinal,
		String entityId,
		String entityType,
		String payloadJson)
		throws SQLException
	{
		when(rs.getString("result_id")).thenReturn(resultId);
		when(rs.getLong("entity_ordinal")).thenReturn(entityOrdinal);
		when(rs.getString("entity_id")).thenReturn(entityId);
		when(rs.getString("entity_type")).thenReturn(entityType);
		when(rs.getString("payload_json")).thenReturn(payloadJson);
		return rs;
	}

	@Nested
	class CollectedEntityH2DaoConstructorTest
	{
		@Test
		void givenDataSourceAndInitializer_whenConstructed_thenInstanceIsCreatedAndInitializerIsCalled()
		{
			DataSource dataSource = mock(DataSource.class);
			boolean[] initialized = new boolean[] { false };

			CollectedEntityH2Dao dao = new CollectedEntityH2Dao(dataSource, () -> initialized[0] = true);

			assertThat(dao).isNotNull();
			assertThat(initialized[0]).isTrue();
		}
	}

	@Nested
	class SaveTest
	{
		@Test
		void givenNullEntity_whenCalled_thenNothingIsSaved() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectedEntityH2Dao dao = newDao(dataSource);

			dao.save(null);

			verify(dataSource, never()).getConnection();
			verify(connection, never()).prepareStatement(anyString());
			verify(statement, never()).executeBatch();
		}

		@Test
		void givenEntity_whenCalled_thenEntityIsSaved() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.getAutoCommit()).thenReturn(true);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectedEntityH2Dao dao = newDao(dataSource);
			CollectedEntityEntity entity = collectedEntity("result-1", 0L, "id-1", "vpc", "{\"id\":\"id-1\"}");

			dao.save(entity);

			verify(statement).setString(1, "result-1");
			verify(statement).setLong(2, 0L);
			verify(statement).setString(3, "id-1");
			verify(statement).setString(4, "vpc");
			verify(statement).setString(5, "{\"id\":\"id-1\"}");
			verify(statement).addBatch();
			verify(statement).executeBatch();
			verify(connection).commit();
			verify(connection).setAutoCommit(false);
			verify(connection).setAutoCommit(true);
		}
	}

	@Nested
	class SaveAllTest
	{
		@Test
		void givenNullCollection_whenCalled_thenNothingIsSaved() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			CollectedEntityH2Dao dao = newDao(dataSource);

			dao.saveAll(null);

			verify(dataSource, never()).getConnection();
		}

		@Test
		void givenEmptyCollection_whenCalled_thenNothingIsSaved() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			CollectedEntityH2Dao dao = newDao(dataSource);

			dao.saveAll(List.of());

			verify(dataSource, never()).getConnection();
		}

		@Test
		void givenCollectionContainingNullEntity_whenCalled_thenNullEntityIsSkipped() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.getAutoCommit()).thenReturn(true);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectedEntityH2Dao dao = newDao(dataSource);

			CollectedEntityEntity entity1 = collectedEntity("result-1", 0L, "id-1", "vpc", "payload-1");
			CollectedEntityEntity entity2 = collectedEntity("result-1", 1L, "id-2", "vpc", "payload-2");

			dao.saveAll(Arrays.asList(entity1, null, entity2));

			verify(statement, org.mockito.Mockito.times(2)).setString(1, "result-1");
			verify(statement).setLong(2, 0L);
			verify(statement).setLong(2, 1L);
			verify(statement, org.mockito.Mockito.times(2)).setString(4, "vpc");
			verify(statement).setString(3, "id-1");
			verify(statement).setString(3, "id-2");
			verify(statement).setString(5, "payload-1");
			verify(statement).setString(5, "payload-2");
			verify(statement, org.mockito.Mockito.times(2)).addBatch();
			verify(statement).executeBatch();
			verify(connection).commit();
		}

		@Test
		void givenEntities_whenCalled_thenAllEntitiesAreBatched() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.getAutoCommit()).thenReturn(true);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectedEntityH2Dao dao = newDao(dataSource);

			CollectedEntityEntity entity1 = collectedEntity("result-1", 0L, "id-1", "vpc", "payload-1");
			CollectedEntityEntity entity2 = collectedEntity("result-1", 1L, "id-2", "subnet", "payload-2");

			dao.saveAll(List.of(entity1, entity2));

			verify(statement, org.mockito.Mockito.times(2)).addBatch();
			verify(statement).executeBatch();
			verify(connection).commit();			
		}

		@Test
		void givenSQLExceptionDuringBatch_whenCalled_thenRuntimeExceptionIsThrownAndTransactionRollsBack() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.getAutoCommit()).thenReturn(true);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			doThrow(new SQLException("boom")).when(statement).executeBatch();

			CollectedEntityH2Dao dao = newBrokenDao(dataSource);

			CollectedEntityEntity entity = collectedEntity("result-1", 0L, "id-1", "vpc", "payload-1");

			assertThatThrownBy(() -> dao.saveAll(List.of(entity)))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to batch save CollectedEntityEntity");

			verify(connection).rollback();
			verify(connection).setAutoCommit(true);
		}

		@Test
		void givenSQLExceptionGettingConnection_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectedEntityH2Dao dao = newBrokenDao(dataSource);
			CollectedEntityEntity entity = collectedEntity("result-1", 0L, "id-1", "vpc", "payload-1");

			assertThatThrownBy(() -> dao.saveAll(List.of(entity)))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to batch save CollectedEntityEntity");
		}
	}

	@Nested
	class FindByResultIdTest
	{
		@Test
		void givenEntitiesReturnedOutOfOrderFromResultSet_whenCalled_thenMappedEntitiesAreReturnedInQueryOrder() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true, true, true, false);

			when(rs.getString("result_id")).thenReturn("result-1", "result-1", "result-1");
			when(rs.getLong("entity_ordinal")).thenReturn(0L, 1L, 2L);
			when(rs.getString("entity_id")).thenReturn("id-1", "id-2", "id-3");
			when(rs.getString("entity_type")).thenReturn("vpc", "vpc", "vpc");
			when(rs.getString("payload_json")).thenReturn("payload-1", "payload-2", "payload-3");

			CollectedEntityH2Dao dao = newDao(dataSource);

			List<CollectedEntityEntity> found = dao.findByResultId("result-1");

			assertThat(found).hasSize(3);
			assertThat(found.get(0).getEntityOrdinal()).isEqualTo(0L);
			assertThat(found.get(1).getEntityOrdinal()).isEqualTo(1L);
			assertThat(found.get(2).getEntityOrdinal()).isEqualTo(2L);
			assertThat(found.get(0).getEntityId()).isEqualTo("id-1");
			assertThat(found.get(1).getEntityId()).isEqualTo("id-2");
			assertThat(found.get(2).getEntityId()).isEqualTo("id-3");

			verify(statement).setString(1, "result-1");
		}

		@Test
		void givenNoMatchingEntities_whenCalled_thenEmptyListIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(false);

			CollectedEntityH2Dao dao = newDao(dataSource);

			assertThat(dao.findByResultId("missing")).isEmpty();
			verify(statement).setString(1, "missing");
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectedEntityH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findByResultId("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectedEntityEntity by resultId");
		}
	}

	@Nested
	class FindPageByResultIdTest
	{
		@Test
		void givenEntitiesExist_whenCalled_thenRequestedPageIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true, true, false);

			when(rs.getString("result_id")).thenReturn("result-1", "result-1");
			when(rs.getLong("entity_ordinal")).thenReturn(1L, 2L);
			when(rs.getString("entity_id")).thenReturn("id-2", "id-3");
			when(rs.getString("entity_type")).thenReturn("vpc", "vpc");
			when(rs.getString("payload_json")).thenReturn("payload-2", "payload-3");

			CollectedEntityH2Dao dao = newDao(dataSource);

			List<CollectedEntityEntity> page = dao.findPageByResultId("result-1", 1L, 2);

			assertThat(page).hasSize(2);
			assertThat(page.get(0).getEntityOrdinal()).isEqualTo(1L);
			assertThat(page.get(1).getEntityOrdinal()).isEqualTo(2L);
			assertThat(page.get(0).getEntityId()).isEqualTo("id-2");
			assertThat(page.get(1).getEntityId()).isEqualTo("id-3");

			verify(statement).setString(1, "result-1");
			verify(statement).setInt(2, 2);
			verify(statement).setLong(3, 1L);
		}

		@Test
		void givenOffsetPastEnd_whenCalled_thenEmptyListIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(false);

			CollectedEntityH2Dao dao = newDao(dataSource);

			assertThat(dao.findPageByResultId("result-1", 10L, 5)).isEmpty();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectedEntityH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findPageByResultId("anything", 0L, 10))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to page CollectedEntityEntity by resultId");
		}
	}

	@Nested
	class FindByProviderIdAndEntityTypeTest
	{
		@Test
		void givenMatchingJoinedRows_whenCalled_thenOnlyMatchingEntitiesAreReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(true, true, true, false);

			when(rs.getString("result_id")).thenReturn("result-1", "result-1", "result-2");
			when(rs.getLong("entity_ordinal")).thenReturn(0L, 1L, 0L);
			when(rs.getString("entity_id")).thenReturn("id-1", "id-2", "id-3");
			when(rs.getString("entity_type")).thenReturn("vpc", "vpc", "vpc");
			when(rs.getString("payload_json")).thenReturn("payload-1", "payload-2", "payload-3");

			CollectedEntityH2Dao dao = newDao(dataSource);

			List<CollectedEntityEntity> found = dao.findByProviderIdAndEntityType("provider-1", "vpc");

			assertThat(found).hasSize(3);
			assertThat(found.get(0).getResultId()).isEqualTo("result-1");
			assertThat(found.get(1).getResultId()).isEqualTo("result-1");
			assertThat(found.get(2).getResultId()).isEqualTo("result-2");
			assertThat(found.get(0).getEntityType()).isEqualTo("vpc");
			assertThat(found.get(1).getEntityType()).isEqualTo("vpc");
			assertThat(found.get(2).getEntityType()).isEqualTo("vpc");
			assertThat(found.get(0).getEntityId()).isEqualTo("id-1");
			assertThat(found.get(1).getEntityId()).isEqualTo("id-2");
			assertThat(found.get(2).getEntityId()).isEqualTo("id-3");

			verify(statement).setString(1, "provider-1");
			verify(statement).setString(2, "vpc");
		}

		@Test
		void givenNoMatchingJoinedData_whenCalled_thenEmptyListIsReturned() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet rs = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(rs);
			when(rs.next()).thenReturn(false);

			CollectedEntityH2Dao dao = newDao(dataSource);

			assertThat(dao.findByProviderIdAndEntityType("provider", "vpc")).isEmpty();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectedEntityH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.findByProviderIdAndEntityType("provider", "vpc"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to find CollectedEntityEntity by providerId and entityType");
		}
	}

	@Nested
	class DeleteByResultIdTest
	{
		@Test
		void givenResultId_whenCalled_thenDeleteIsExecuted() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectedEntityH2Dao dao = newDao(dataSource);

			dao.deleteByResultId("result-1");

			verify(statement).setString(1, "result-1");
			verify(statement).executeUpdate();
		}

		@Test
		void givenNoEntitiesForResultId_whenCalled_thenNothingFails() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			CollectedEntityH2Dao dao = newDao(dataSource);

			dao.deleteByResultId("missing");

			verify(statement).setString(1, "missing");
			verify(statement).executeUpdate();
		}

		@Test
		void givenSQLException_whenCalled_thenRuntimeExceptionIsThrown() throws Exception
		{
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

			CollectedEntityH2Dao dao = newBrokenDao(dataSource);

			assertThatThrownBy(() -> dao.deleteByResultId("anything"))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("Failed to delete CollectedEntityEntity by resultId");
		}
	}
}