package org.wonderdb.client.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DreamDBResultSetMetadata implements ResultSetMetaData {
	List<ResultSetColumn> metadata = new ArrayList<DreamDBResultSetMetadata.ResultSetColumn>();
	
	public DreamDBResultSetMetadata(List<ResultSetColumn> list) {
		this.metadata = list;
	}
	
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getColumnCount() throws SQLException {
		return metadata.size();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return metadata.get(column).columnName;
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return metadata.get(column).columnName;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return metadata.get(column).type;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getScale(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public String getTableName(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int isNullable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		throw new RuntimeException("Method not supported");
	}
	
	public static class ResultSetColumn {
		String columnName;
		int type;
	}
}
