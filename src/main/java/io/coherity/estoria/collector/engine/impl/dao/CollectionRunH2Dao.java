package io.coherity.estoria.collector.engine.impl.dao;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CollectionRunH2Dao implements CollectionRunDao
{
	private final String jdbcUrl = "jdbc:h2:./collector-db";
	private final String user = "sa";
	private final String password = "";

	public CollectionRunH2Dao()
	{
		try (Connection conn = getConnection(); Statement stmt = conn.createStatement())
		{
			stmt.execute("""
					    CREATE TABLE IF NOT EXISTS collection_run (
					        run_id varchar(128) primary key,
					        provider_id varchar(128) not null,
					        collection_scope varchar(128) not null,
					        status varchar(32) not null,
					        run_start_time timestamp with time zone not null,
					        run_end_time timestamp with time zone,
					        total_collector_count bigint not null default 0,
					        successful_collector_count bigint not null default 0,
					        failed_collector_count bigint not null default 0,
					        total_entity_count bigint not null default 0,
					        failure_message clob,
					        failure_exception_class varchar(512)
					    )
					""");
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to initialize collection_run table", e);
		}
	}

	private Connection getConnection() throws SQLException
	{
		return DriverManager.getConnection(jdbcUrl, user, password);
	}

	@Override
	public void save(CollectionRunEntity run)
	{
		String sql = """
				    MERGE INTO collection_run (run_id, provider_id, collection_scope, status, run_start_time, run_end_time, total_collector_count, successful_collector_count, failed_collector_count, total_entity_count, failure_message, failure_exception_class)
				    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, run.getRunId());
			ps.setString(2, run.getProviderId());
			ps.setString(3, run.getCollectionScope());
			ps.setString(4, run.getStatus());
			ps.setTimestamp(5, Timestamp.from(run.getRunStartTime()));
			ps.setTimestamp(6, run.getRunEndTime() != null ? Timestamp.from(run.getRunEndTime()) : null);
			ps.setLong(7, run.getTotalCollectorCount());
			ps.setLong(8, run.getSuccessfulCollectorCount());
			ps.setLong(9, run.getFailedCollectorCount());
			ps.setLong(10, run.getTotalEntityCount());
			ps.setString(11, run.getFailureMessage());
			ps.setString(12, run.getFailureExceptionClass());
			ps.executeUpdate();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to save CollectionRunEntity", e);
		}
	}

	@Override
	public Optional<CollectionRunEntity> findById(String runId)
	{
		String sql = "SELECT * FROM collection_run WHERE run_id = ?";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, runId);
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				return Optional.of(mapRow(rs));
			}
			return Optional.empty();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectionRunEntity by id", e);
		}
	}

	@Override
	public List<CollectionRunEntity> findByProviderId(String providerId)
	{
		String sql = "SELECT * FROM collection_run WHERE provider_id = ?";
		List<CollectionRunEntity> results = new ArrayList<>();
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, providerId);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				results.add(mapRow(rs));
			}
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectionRunEntity by providerId", e);
		}
		return results;
	}

	@Override
	public List<CollectionRunEntity> findByStatus(String status)
	{
		String sql = "SELECT * FROM collection_run WHERE status = ?";
		List<CollectionRunEntity> results = new ArrayList<>();
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, status);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				results.add(mapRow(rs));
			}
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to find CollectionRunEntity by status", e);
		}
		return results;
	}

	@Override
	public List<CollectionRunEntity> findAll()
	{
		String sql = "SELECT * FROM collection_run";
		List<CollectionRunEntity> results = new ArrayList<>();
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
			throw new RuntimeException("Failed to find all CollectionRunEntity", e);
		}
		return results;
	}

	@Override
	public void updateStatus(String runId, String status, Instant endTime, String failureMessage,
			String failureExceptionClass)
	{
		String sql = "UPDATE collection_run SET status = ?, run_end_time = ?, failure_message = ?, failure_exception_class = ? WHERE run_id = ?";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, status);
			ps.setTimestamp(2, endTime != null ? Timestamp.from(endTime) : null);
			ps.setString(3, failureMessage);
			ps.setString(4, failureExceptionClass);
			ps.setString(5, runId);
			ps.executeUpdate();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to update CollectionRunEntity status", e);
		}
	}

	@Override
	public void delete(String runId)
	{
		String sql = "DELETE FROM collection_run WHERE run_id = ?";
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql))
		{
			ps.setString(1, runId);
			ps.executeUpdate();
		} catch (SQLException e)
		{
			throw new RuntimeException("Failed to delete CollectionRunEntity", e);
		}
	}

	private CollectionRunEntity mapRow(ResultSet rs) throws SQLException
	{
		CollectionRunEntity entity = new CollectionRunEntity();
		entity.setRunId(rs.getString("run_id"));
		entity.setProviderId(rs.getString("provider_id"));
		entity.setCollectionScope(rs.getString("collection_scope"));
		entity.setStatus(rs.getString("status"));
		entity.setRunStartTime(rs.getTimestamp("run_start_time").toInstant());
		Timestamp endTs = rs.getTimestamp("run_end_time");
		entity.setRunEndTime(endTs != null ? endTs.toInstant() : null);
		entity.setTotalCollectorCount(rs.getLong("total_collector_count"));
		entity.setSuccessfulCollectorCount(rs.getLong("successful_collector_count"));
		entity.setFailedCollectorCount(rs.getLong("failed_collector_count"));
		entity.setTotalEntityCount(rs.getLong("total_entity_count"));
		entity.setFailureMessage(rs.getString("failure_message"));
		entity.setFailureExceptionClass(rs.getString("failure_exception_class"));
		return entity;
	}
}
