package org.eclipse.birt.report.engine.emitter.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.birt.core.exception.BirtException;
import org.eclipse.birt.report.engine.content.IBandContent;
import org.eclipse.birt.report.engine.content.IDataContent;
import org.eclipse.birt.report.engine.content.IElement;
import org.eclipse.birt.report.engine.content.IReportContent;
import org.eclipse.birt.report.engine.content.IRowContent;
import org.eclipse.birt.report.engine.content.ITableContent;
import org.eclipse.birt.report.engine.emitter.ContentEmitterAdapter;
import org.eclipse.birt.report.engine.emitter.IEmitterServices;
import org.eclipse.birt.report.engine.presentation.ContentEmitterVisitor;

public class JdbcReportEmitter extends ContentEmitterAdapter
{

	protected ContentEmitterVisitor contentVisitor;
	protected IEmitterServices service;

	private boolean exportElement;
	private boolean isTruncateBeforeLoad;
	private boolean isTableExists = false;
	private boolean isTableNameNull = false;
	private String tableName;
	private int tableDepth;
	private int columnCount;
	private int rowCount;
	private Connection conn;
	private String insertStatement;
	private List<Object> rowList = new ArrayList<Object>();
	private PreparedStatement stmt;

	public JdbcReportEmitter()
	{
		this.contentVisitor = new ContentEmitterVisitor(this);
	}

	@Override
	public void initialize(IEmitterServices service) throws BirtException
	{
		this.service = service;
	}

	@Override
	public void start(IReportContent report) throws BirtException
	{
		setupDatabaseConnection();
	}

	@Override
	public void end(IReportContent report) throws BirtException
	{
		closeDatabaseConnection();
	}

	@Override
	public void startTable(ITableContent table) throws BirtException
	{
		assert table != null;
		if (table.getName() == null || table.getName().equals(""))
		{
			isTableNameNull = true;
			return;
		}
		{
			isTableNameNull = false;
		}
		if (!table.getName().equals(tableName) && conn != null)
		{
			tableName = table.getName();
			columnCount = table.getColumnCount();
			tableDepth = 0;

			ResultSet tables;
			try
			{
				tables = conn.getMetaData().getTables(null, null, tableName, null);

				if (tables.next())
				{
					insertStatement = prepareInsertStatement(tableName, columnCount);
					stmt = conn.prepareStatement(insertStatement);
					isTableExists = true;
				}
				else
				{
					isTableExists = false;
				}
			}
			catch (SQLException e)
			{
				throw new BirtException(e.getMessage());
			}
		}
		if (tableDepth == 0 && isTruncateBeforeLoad && isTableExists)
		{
			String statement = prepareDeleteStatement();
			try
			{
				deleteFromDb(statement);
			}
			catch (SQLException e)
			{
				throw new BirtException(e.getMessage());
			}
		}
		tableDepth++;
		rowCount = 0;
	}

	@Override
	public void endTable(ITableContent table) throws BirtException
	{
		try
		{
			if (stmt != null)
			{
				stmt.executeBatch();
				stmt.clearBatch();
			}
		}
		catch (SQLException e)
		{
			throw new BirtException(e.getMessage());
		}
	}

	@Override
	public void startRow(IRowContent row) throws BirtException
	{
		assert row != null;
		exportElement = isExportElement(row);
		rowCount++;
	}

	@Override
	public void endRow(IRowContent row) throws BirtException
	{
		if (isTableNameNull)
			return;
		try
		{
			if (exportElement && isTableExists)
				insertToBatch();
			exportElement = true;
			rowList.clear();
		}
		catch (SQLException e)
		{
			throw new BirtException(e.getMessage());
		}

	}

	@Override
	public void startData(IDataContent data) throws BirtException
	{
		if (exportElement)
			rowList.add(data.getValue());
	}

	private void setupDatabaseConnection() throws BirtException
	{
		try
		{
			Object obj = this.service.getOption(JdbcRenderOption.TRUNCATE_BEFORE_LOAD);
			if (obj != null)
			{
				isTruncateBeforeLoad = (Boolean) obj;
			}

			Object connectionUrlObj = this.service.getOption(JdbcRenderOption.CONNECTION_URL);
			if (connectionUrlObj != null)
			{
				Class.forName("org.sqlite.JDBC");
				Properties connectionProps = new Properties();
				connectionProps.put("user", (String) this.service.getOption(JdbcRenderOption.USERNAME));
				connectionProps.put("password", (String) this.service.getOption(JdbcRenderOption.PASSWORD));
				conn = DriverManager.getConnection((String) connectionUrlObj, connectionProps);
				conn.setAutoCommit(false);
			}
		}
		catch (ClassNotFoundException e)
		{
			throw new BirtException(e.getMessage());
		}
		catch (SQLException e)
		{
			throw new BirtException(e.getMessage());
		}
	}

	private void closeDatabaseConnection() throws BirtException
	{
		try
		{
			if (stmt != null)
				stmt.close();
			if (conn != null)
			{
				conn.commit();
				conn.close();
			}
		}
		catch (SQLException e)
		{
			throw new BirtException(e.getMessage());
		}
	}

	private boolean isExportElement(IRowContent row)
	{
		IElement parent = row.getParent();
		if (!(parent instanceof IBandContent))
			return true;

		IBandContent band = (IBandContent) parent;
		if (band.getBandType() == IBandContent.BAND_FOOTER || band.getBandType() == IBandContent.BAND_HEADER)
			return false;
		return true;
	}

	private String prepareInsertStatement(String tableName, int columnCount)
	{
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(tableName).append(" values(");
		for (int i = 0; i < columnCount; i++)
		{
			if (i == columnCount - 1)
			{
				sb.append("?)");
			}
			else
			{
				sb.append("?,");
			}
		}

		return sb.toString();
	}

	private String prepareDeleteStatement()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("delete from ").append(tableName);
		return sb.toString();
	}

	private void insertToBatch() throws SQLException
	{
		for (int i = 1; i <= rowList.size(); i++)
		{
			Object value = rowList.get(i - 1);
			if (value instanceof java.util.Date)
			{
				java.util.Date utilDate = (java.util.Date) value;
				stmt.setTimestamp(i, new Timestamp(utilDate.getTime()));
			}
			else
			{
				stmt.setObject(i, value);
			}
		}
		stmt.addBatch();
	}

	private void deleteFromDb(String statement) throws SQLException
	{
		if (conn != null)
		{
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(statement);
			stmt.close();
		}
	}
}
