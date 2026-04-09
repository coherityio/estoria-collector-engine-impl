package io.coherity.estoria.collector.engine.impl.dao;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.coherity.estoria.collector.engine.impl.config.ApplicationConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class H2DatabaseInitializer
{
    private static final String SCHEMA_RESOURCE_PATH = "db/h2/schema.sql";

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

            log.debug("Initializing H2 database using schema resource: {}", SCHEMA_RESOURCE_PATH);

            String schema = loadSchemaFromClasspath();
            applySchema(schema);
            applyMigrations();

            initialized = true;
            log.debug("H2 database initialization complete.");
        }
    }

    private static String loadSchemaFromClasspath()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try (InputStream inputStream = classLoader.getResourceAsStream(SCHEMA_RESOURCE_PATH))
        {
            if (inputStream == null)
            {
                throw new RuntimeException(
                    "Schema resource not found on classpath: " + SCHEMA_RESOURCE_PATH);
            }

            String schema = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            log.debug("Loaded schema resource ({} bytes): {}", schema.length(), SCHEMA_RESOURCE_PATH);
            return schema;
        }
        catch (IOException e)
        {
            throw new RuntimeException(
                "Failed to read schema resource from classpath: " + SCHEMA_RESOURCE_PATH, e);
        }
    }

    private static void applySchema(String schema)
    {
        String jdbcUrl = ApplicationConfig.getDBJDBCUrlString();
        String user = ApplicationConfig.getDBUserString();
        String pass = ApplicationConfig.getDBPassString();

        log.debug("Applying schema to H2 database at URL: {}", jdbcUrl);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, pass);
            Statement stmt = conn.createStatement())
        {
            for (String statement : splitStatements(schema))
            {
                String trimmed = statement.strip();
                if (!trimmed.isEmpty())
                {
                    log.debug("Executing DDL statement: {}", trimmed.substring(0, Math.min(trimmed.length(), 80)));
                    stmt.execute(trimmed);
                }
            }

            log.debug("Schema applied successfully.");
        }
        catch (SQLException e)
        {
            log.error("Failed to apply H2 schema", e);
            throw new RuntimeException("Failed to apply H2 schema", e);
        }
    }

    private static void applyMigrations()
    {
        String jdbcUrl = ApplicationConfig.getDBJDBCUrlString();
        String user = ApplicationConfig.getDBUserString();
        String pass = ApplicationConfig.getDBPassString();

        log.debug("Checking for required schema migrations.");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, pass);
            Statement stmt = conn.createStatement())
        {
            if (!columnExists(conn, "COLLECTION_RUN", "PROVIDER_CONTEXT"))
            {
                log.info("Migration required: adding provider_context to collection_run.");
                try
                {
                    stmt.execute(
                        "ALTER TABLE collection_run ALTER COLUMN collection_scope RENAME TO provider_context");
                    log.info("Migration complete: collection_scope renamed to provider_context.");
                }
                catch (SQLException renameEx)
                {
                    log.warn(
                        "Could not rename collection_scope; adding provider_context as new column.", renameEx);
                    stmt.execute(
                        "ALTER TABLE collection_run ADD COLUMN IF NOT EXISTS provider_context clob");
                    log.info("Migration complete: provider_context column added.");
                }
            }
            else
            {
                log.debug("No migrations required; schema is up to date.");
            }
        }
        catch (SQLException e)
        {
            log.error("Failed to apply H2 migrations", e);
            throw new RuntimeException("Failed to apply H2 migrations", e);
        }
    }

    private static boolean columnExists(Connection conn, String tableName, String columnName)
        throws SQLException
    {
        try (ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, columnName))
        {
            return rs.next();
        }
    }

    private static String[] splitStatements(String schema)
    {
        return schema.split(";");
    }

    private H2DatabaseInitializer()
    {
        // utility
    }
}