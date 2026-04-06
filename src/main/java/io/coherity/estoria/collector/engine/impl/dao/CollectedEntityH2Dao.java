package io.coherity.estoria.collector.engine.impl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import io.coherity.estoria.collector.engine.impl.cli.ApplicationConfig;

public class CollectedEntityH2Dao implements CollectedEntityDao
{
	private final DataSource dataSource;

	CollectedEntityH2Dao(DataSource dataSource, SchemaInitializer schemaInitializer)
	{
		this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
		Objects.requireNonNull(schemaInitializer, "schemaInitializer must not be null").init();
	}

	CollectedEntityH2Dao(String jdbcUrl, String user, String pass)
	{
		this(new SimpleDriverManagerDataSource(jdbcUrl, user, pass), H2DatabaseInitializer::init);
	}

	public CollectedEntityH2Dao()
	{
		this(
			ApplicationConfig.getDBJDBCUrlString(),
			ApplicationConfig.getDBUserString(),
			ApplicationConfig.getDBPassString());
	}

	private Connection getConnection() throws SQLException
	{
		return dataSource.getConnection();
	}

	@Override
	public void save(CollectedEntityEntity entity)
	{
		if (entity == null)
		{
			return;
		}
		saveAll(Collections.singletonList(entity));
	}

	@Override
	public void saveAll(Collection<CollectedEntityEntity> entities)
	{
		if (entities == null || entities.isEmpty())
		{
			return;
		}

		String sql = """
				MERGE INTO collected_entity (result_id, entity_ordinal, entity_id, entity_type, payload_json)
				VALUES (?, ?, ?, ?, ?)
			""";

		try (Connection conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(sql))
		{
			boolean originalAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			try
			{
				for (CollectedEntityEntity entity : entities)
				{
					if (entity == null)
					{
						continue;
					}

					ps.setString(1, entity.getResultId());
					ps.setLong(2, entity.getEntityOrdinal());
					ps.setString(3, entity.getEntityId());
					ps.setString(4, entity.getEntityType());
					ps.setString(5, entity.getPayloadJson());
					ps.addBatch();
				}

				ps.executeBatch();
				conn.commit();
			}
			catch (SQLException e)
			{
				conn.rollback();
				throw new RuntimeException("Failed to batch save CollectedEntityEntity", e);
			}
			finally
			{
				conn.setAutoCommit(originalAutoCommit);
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException("Failed to batch save CollectedEntityEntity", e);
		}
	}

	@Override
	public List<CollectedEntityEntity> findByResultId(String resultId)
	{
		String sql = "SELECT * FROM collected_entity WHERE result_id = ? ORDER BY entity_ordinal";
		List<CollectedEntityEntity> results = new ArrayList<>();

		try (Connection conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, resultId);
			try (ResultSet rs = ps.executeQuery())
			{
				while (rs.next())
				{
					results.add(mapRow(rs));
				}
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectedEntityEntity by resultId", e);
		}

		return results;
	}

	@Override
	public List<CollectedEntityEntity> findPageByResultId(String resultId, long offset, int limit)
	{
		String sql = "SELECT * FROM collected_entity WHERE result_id = ? ORDER BY entity_ordinal LIMIT ? OFFSET ?";
		List<CollectedEntityEntity> results = new ArrayList<>();

		try (Connection conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, resultId);
			ps.setInt(2, limit);
			ps.setLong(3, offset);

			try (ResultSet rs = ps.executeQuery())
			{
				while (rs.next())
				{
					results.add(mapRow(rs));
				}
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException("Failed to page CollectedEntityEntity by resultId", e);
		}

		return results;
	}

	@Override
	public List<CollectedEntityEntity> findByProviderIdAndEntityType(String providerId, String entityType)
	{
		String sql = """
				SELECT ce.*
				FROM collected_entity ce
				JOIN collection_result cr ON ce.result_id = cr.result_id
				JOIN collection_run r ON cr.run_id = r.run_id
				WHERE r.provider_id = ? AND ce.entity_type = ?
				ORDER BY r.run_start_time, cr.collection_start_time, ce.entity_ordinal
			""";

		List<CollectedEntityEntity> results = new ArrayList<>();

		try (Connection conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, providerId);
			ps.setString(2, entityType);

			try (ResultSet rs = ps.executeQuery())
			{
				while (rs.next())
				{
					results.add(mapRow(rs));
				}
			}
		}
		catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectedEntityEntity by providerId and entityType", e);
		}

		return results;
	}

	@Override
	public void deleteByResultId(String resultId)
	{
		String sql = "DELETE FROM collected_entity WHERE result_id = ?";

		try (Connection conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, resultId);
			ps.executeUpdate();
		}
		catch (SQLException e)
		{
			throw new RuntimeException("Failed to delete CollectedEntityEntity by resultId", e);
		}
	}

	private CollectedEntityEntity mapRow(ResultSet rs) throws SQLException
	{
		CollectedEntityEntity entity = new CollectedEntityEntity();
		entity.setResultId(rs.getString("result_id"));
		entity.setEntityOrdinal(rs.getLong("entity_ordinal"));
		entity.setEntityId(rs.getString("entity_id"));
		entity.setEntityType(rs.getString("entity_type"));
		entity.setPayloadJson(rs.getString("payload_json"));
		return entity;
	}


}