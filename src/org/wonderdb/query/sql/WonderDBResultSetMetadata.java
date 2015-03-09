package org.wonderdb.query.sql;

/*******************************************************************************
 *    Copyright 2013 Vilas Athavale
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WonderDBResultSetMetadata implements ResultSetMetaData {
	List<ResultSetColumn> metadata = new ArrayList<WonderDBResultSetMetadata.ResultSetColumn>();
	
	public WonderDBResultSetMetadata(List<ResultSetColumn> list) {
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
