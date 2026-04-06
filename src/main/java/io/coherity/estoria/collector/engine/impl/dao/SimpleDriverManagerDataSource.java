package io.coherity.estoria.collector.engine.impl.dao;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class SimpleDriverManagerDataSource implements DataSource
{
	private final String jdbcUrl;
	private final String user;
	private final String pass;

	public SimpleDriverManagerDataSource(String jdbcUrl, String user, String pass)
	{
		this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl must not be null");
		this.user = user;
		this.pass = pass;
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		return DriverManager.getConnection(jdbcUrl, user, pass);
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException
	{
		return DriverManager.getConnection(jdbcUrl, username, password);
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Not implemented");
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Not implemented");
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Not implemented");
	}

	@Override
	public int getLoginTimeout() throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Not implemented");
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("Not implemented");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Not implemented");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}
}