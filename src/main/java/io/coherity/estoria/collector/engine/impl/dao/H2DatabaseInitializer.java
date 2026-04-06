package io.coherity.estoria.collector.engine.impl.dao;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;

/**
 * Initializes the local H2 database once per JVM, and only creates tables
 * when the database has not yet been created on disk.
 */
@Slf4j
class H2DatabaseInitializer
{
	private static final String JDBC_URL = "jdbc:h2:./collector-db";
	private static final String USER = "sa";
	private static final String PASSWORD = "";

	// H2 will typically create one of these files for the local database
	private static final Path MV_DB_FILE = Path.of("collector-db.mv.db");
	private static final Path H2_DB_FILE = Path.of("collector-db.h2.db");

	private static volatile boolean initialized = false;

	static void init()
	{
		if (initialized)
		{
			log.debug("H2 database already initialized; skipping initialization.");
			return;
		}

		synchronized (H2DatabaseInitializer.class)
		{
			if (initialized)
			{
				log.debug("H2 database already initialized inside synchronized block; skipping initialization.");
				return;
			}

			boolean dbExists = Files.exists(MV_DB_FILE) || Files.exists(H2_DB_FILE);
			if (!dbExists)
			{
				log.debug("No existing H2 database files found; creating schema at URL: {}", JDBC_URL);
				createSchema();
			}
			else
			{
				log.debug("Detected existing H2 database files; skipping schema creation.");
			}

			initialized = true;
			log.debug("H2 database initialization complete.");
		}
	}

	private static void createSchema()
	{
		try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
				var stmt = conn.createStatement())
		{
			log.debug("Creating H2 schema (tables: collection_run, collection_result, collected_entity).");
			// Run the same DDL statements that were previously executed in each DAO
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

			stmt.execute("""
				    CREATE TABLE IF NOT EXISTS collection_result (
				        result_id varchar(128) primary key,
				        run_id varchar(128) not null,
				        collector_id varchar(128) not null,
				        entity_type varchar(256) not null,
				        status varchar(32) not null,
				        entity_count bigint not null default 0,
				        collection_start_time timestamp with time zone not null,
				        collection_end_time timestamp with time zone,
				        failure_message clob,
				        failure_exception_class varchar(512)
				    )
				""");

			stmt.execute("""
				    CREATE TABLE IF NOT EXISTS collected_entity (
				        result_id varchar(128) not null,
				        entity_ordinal bigint not null,
				        entity_id varchar(512),
				        entity_type varchar(256) not null,
				        payload_json clob not null,
				        constraint pk_collected_entity primary key (result_id, entity_ordinal)
				    )
				""");
		}
		catch (SQLException e)
		{
			log.error("Failed to initialize H2 schema", e);
			throw new RuntimeException("Failed to initialize H2 schema", e);
		}
	}

	private H2DatabaseInitializer()
	{
		// utility
	}
}
