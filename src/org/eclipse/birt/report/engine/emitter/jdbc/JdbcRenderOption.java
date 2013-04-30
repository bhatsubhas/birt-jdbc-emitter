package org.eclipse.birt.report.engine.emitter.jdbc;

import org.eclipse.birt.report.engine.api.RenderOption;

public class JdbcRenderOption extends RenderOption
{
	public static final String JDBC_EMITTER_ID = "org.eclipse.birt.report.engine.emitter.jdbc";
	public static final String CONNECTION_URL = "connection_url";
	public static final String TRUNCATE_BEFORE_LOAD = "truncate_before_load";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";

	public String getConnectionUrl()
	{
		return getStringOption(CONNECTION_URL);
	}

	public void setConnectionUrl(String url)
	{
		setOption(CONNECTION_URL, url);
	}

	public boolean isTruncateBeforeLoad()
	{
		return getBooleanOption(TRUNCATE_BEFORE_LOAD, true);
	}

	public void setTruncateBeforeLoad(boolean value)
	{
		setOption(TRUNCATE_BEFORE_LOAD, value);
	}

	public String getUsername()
	{
		return getStringOption(USERNAME);
	}

	public void setUsername(String username)
	{
		setOption(USERNAME, username);
	}

	public String getPassword()
	{
		return getStringOption(PASSWORD);
	}

	public void setPassword(String password)
	{
		setOption(PASSWORD, password);
	}
}
