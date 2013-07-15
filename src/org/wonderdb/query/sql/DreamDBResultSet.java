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
package org.wonderdb.query.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.wonderdb.query.sql.DreamDBPreparedStatement.ResultSetValue;
import org.wonderdb.query.sql.DreamDBResultSetMetadata.ResultSetColumn;


public class DreamDBResultSet implements ResultSet {
	DreamDBResultSetMetadata metadata = null;
	DreamDBConnection connection = null;
	DreamDBPreparedStatement stmt = null;
	List<ResultSetValue> valueList = null;
	int currentPosn = -1;
	boolean closed = false;
	
	public DreamDBResultSet(DreamDBConnection connection, DreamDBPreparedStatement stmt, DreamDBResultSetMetadata metadata, List<ResultSetValue> list) {
		this.connection = connection;
		this.metadata = metadata;
		this.valueList = list;
		this.stmt = stmt;
	}
	
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void afterLast() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		for (int i = 0; i < metadata.metadata.size(); i++) {
			ResultSetColumn rsc = metadata.metadata.get(i);
			if (columnLabel.equals(rsc.columnName)) {
				return i;
			}
		}
		throw new SQLException("Column not found");
	}

	@Override
	public boolean first() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		return (byte[]) rsv.valueList.get(columnIndex);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getBytes(i);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		return (Date) rsv.valueList.get(columnIndex);
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getDate(i);
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		Double d = (Double) rsv.valueList.get(columnIndex);
		if (d == null) {
			return 0;
		}
		return d;
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getDouble(i);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		Float f = (Float) rsv.valueList.get(columnIndex);
		if (f == null) {
			return 0;
		}
		return f;
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getFloat(i);
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		Integer i = (Integer) rsv.valueList.get(columnIndex);
		if (i == null) {
			return 0;
		}
		return i;
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getInt(i);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		Long l = (Long) rsv.valueList.get(columnIndex);
		if (l == null) {
			return 0;
		}
		return l;
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getLong(i);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRow() throws SQLException {
		return currentPosn;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		Integer i = (Integer) rsv.valueList.get(columnIndex);
		if (i != null) {
			return i.shortValue();
		}
		return 0;
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getShort(i);
	}

	@Override
	public Statement getStatement() throws SQLException {
		return stmt;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		return (String) rsv.valueList.get(columnIndex);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getString(i);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		return (Time) rsv.valueList.get(columnIndex);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getTime(i);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		return (Timestamp) rsv.valueList.get(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getTimestamp(i);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		ResultSetValue rsv = valueList.get(currentPosn);
		String s = (String) rsv.valueList.get(columnIndex);
		URL url = null;
		try {
			url = new URL(s);
		} catch (MalformedURLException e) {
			throw new SQLException(e);
		}
		return url;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		int i = findColumn(columnLabel);
		return getURL(i);
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public boolean isFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public boolean next() throws SQLException {
		currentPosn++;
		return valueList.size() > currentPosn;
	}

	@Override
	public boolean previous() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw new SQLException("Method not found");
	}

	@Override
	public boolean wasNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
}
