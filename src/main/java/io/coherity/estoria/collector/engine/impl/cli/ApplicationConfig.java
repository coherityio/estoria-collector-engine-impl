package io.coherity.estoria.collector.engine.impl.cli;

import org.apache.commons.configuration2.CompositeConfiguration;

import io.coherity.shared.common.config.CascadingConfigurationFactory;

public class ApplicationConfig
{
	private static final String[] CONFIG_FILE_NAMES = 
			new String[] {
					"collector-engine.yml", 
					"collector-engine.properties"
			};
	private static final CompositeConfiguration config;
	static
	{
		config = CascadingConfigurationFactory.createCascadingConfiguration(CONFIG_FILE_NAMES);
	}
	
	public static final String COLLECTOR_ENGINE_LOCAL_H2_BASE_PATH = "io.coherity.estoria.collector.engine.impl.h2";
	public static final String H2_JDBC_URL_KEY = COLLECTOR_ENGINE_LOCAL_H2_BASE_PATH + ".jdbc.url";
	public static final String H2_USER_KEY = COLLECTOR_ENGINE_LOCAL_H2_BASE_PATH + ".user";
	public static final String H2_PASS_KEY = COLLECTOR_ENGINE_LOCAL_H2_BASE_PATH + ".pass";
	
	public static String getDBJDBCUrlString()
	{
		return (config.getString(H2_JDBC_URL_KEY) != null) ? config.getString(H2_JDBC_URL_KEY) : "";
	}
	
	public static String getDBUserString()
	{
		return (config.getString(H2_USER_KEY) != null) ? config.getString(H2_USER_KEY) : "";
	}
	
	public static String getDBPassString()
	{
		return (config.getString(H2_PASS_KEY) != null) ? config.getString(H2_PASS_KEY) : "";
	}
	
}