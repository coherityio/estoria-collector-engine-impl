package io.coherity.estoria.collector.engine.impl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import io.coherity.estoria.collector.engine.impl.cli.ApplicationConfig;

public class CollectionResultH2Dao implements CollectionResultDao
{
	private final DataSource dataSource;

	CollectionResultH2Dao(DataSource dataSource, SchemaInitializer schemaInitializer)
	{
		this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
		Objects.requireNonNull(schemaInitializer, "schemaInitializer must not be null").init();
	}

	CollectionResultH2Dao(String jdbcUrl, String user, String pass)
	{
		this(new SimpleDriverManagerDataSource(jdbcUrl, user, pass), H2DatabaseInitializer::init);
	}

	public CollectionResultH2Dao()
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
	public void save(CollectionResultEntity result)
	{
		String sql = """
				    MERGE INTO collection_result (result_id, run_id, collector_id, collector_context, entity_type, status, entity_count, collection_start_time, collection_end_time, failure_message, failure_exception_class)
				    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, result.getResultId());
			ps.setString(2, result.getRunId());
			ps.setString(3, result.getCollectorId());
			ps.setString(4, result.getCollectorContext());
			ps.setString(5, result.getEntityType());
			ps.setString(6, result.getStatus());
			ps.setLong(7, result.getEntityCount());
			ps.setTimestamp(8, Timestamp.from(result.getCollectionStartTime()));
			ps.setTimestamp(9,
					result.getCollectionEndTime() != null ? Timestamp.from(result.getCollectionEndTime()) : null);
			ps.setString(10, result.getFailureMessage());
			ps.setString(11, result.getFailureExceptionClass());
			ps.executeUpdate();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to save CollectionResultEntity", e);
		}
	}

	@Override
	public Optional<CollectionResultEntity> findById(String resultId)
	{
		String sql = "SELECT * FROM collection_result WHERE result_id = ?";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, resultId);
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				return Optional.of(mapRow(rs));
			}
			return Optional.empty();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectionResultEntity by id", e);
		}
	}

	@Override
	public List<CollectionResultEntity> findByRunId(String runId)
	{
		String sql = "SELECT * FROM collection_result WHERE run_id = ?";
		List<CollectionResultEntity> results = new ArrayList<>();
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, runId);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				results.add(mapRow(rs));
			}
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectionResultEntity by runId", e);
		}
		return results;
	}

	@Override
	public List<CollectionResultEntity> findByEntityType(String entityType)
	{
		String sql = "SELECT * FROM collection_result WHERE entity_type = ?";
		List<CollectionResultEntity> results = new ArrayList<>();
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, entityType);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				results.add(mapRow(rs));
			}
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectionResultEntity by entityType", e);
		}
		return results;
	}

	@Override
	public List<CollectionResultEntity> findAll()
	{
		String sql = "SELECT * FROM collection_result";
		List<CollectionResultEntity> results = new ArrayList<>();
		try (Connection conn = getConnection();
				PreparedStatement ps = conn.prepareStatement(sql);
				ResultSet rs = ps.executeQuery())
		{
			while (rs.next())
			{
				results.add(mapRow(rs));
			}
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to find all CollectionResultEntity", e);
		}
		return results;
	}

	@Override
	public void updateStatus(String resultId, String status, String failureMessage, String failureExceptionClass)
	{
		String sql = "UPDATE collection_result SET status = ?, failure_message = ?, failure_exception_class = ? WHERE result_id = ?";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, status);
			ps.setString(2, failureMessage);
			ps.setString(3, failureExceptionClass);
			ps.setString(4, resultId);
			ps.executeUpdate();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to update CollectionResultEntity status", e);
		}
	}

	@Override
	public void delete(String resultId)
	{
		String sql = "DELETE FROM collection_result WHERE result_id = ?";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, resultId);
			ps.executeUpdate();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to delete CollectionResultEntity", e);
		}
	}

	private CollectionResultEntity mapRow(ResultSet rs) throws SQLException
	{
		CollectionResultEntity entity = new CollectionResultEntity();
		entity.setResultId(rs.getString("result_id"));
		entity.setRunId(rs.getString("run_id"));
		entity.setCollectorId(rs.getString("collector_id"));
		entity.setCollectorContext(rs.getString("collector_context"));
		entity.setEntityType(rs.getString("entity_type"));
		entity.setStatus(rs.getString("status"));
		entity.setEntityCount(rs.getLong("entity_count"));
		entity.setCollectionStartTime(rs.getTimestamp("collection_start_time").toInstant());
		Timestamp endTs = rs.getTimestamp("collection_end_time");
		entity.setCollectionEndTime(endTs != null ? endTs.toInstant() : null);
		entity.setFailureMessage(rs.getString("failure_message"));
		entity.setFailureExceptionClass(rs.getString("failure_exception_class"));
		return entity;
	}
}
