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
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.wonderdb.query.sql.DreamDBConnection.WireData;
import org.wonderdb.query.sql.DreamDBResultSetMetadata.ResultSetColumn;


public class DreamDBPreparedStatement implements PreparedStatement {
	
	DreamDBConnection connection = null;
	String sql = null;
	List<BindValue> bindValues = new ArrayList<DreamDBPreparedStatement.BindValue>();
	int updateCount = -1;
	boolean closed = false;
	DreamDBResultSetMetadata metadata = null;
	ChannelBuffer buffer = null;
	
	DreamDBPreparedStatement(DreamDBConnection connection, String sql) {
		this.connection = connection;
		this.sql = sql;
	}
	
	DreamDBPreparedStatement(DreamDBConnection connection, ChannelBuffer buffer) {
		this.connection = connection;
		this.buffer = buffer;
	}
	
	@Override
	public void addBatch(String arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void cancel() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public void close() throws SQLException {
	}

	@Override
	public boolean execute(String arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean execute(String arg0, int arg1) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean execute(String arg0, int[] arg1) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean execute(String arg0, String[] arg1) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int[] executeBatch() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	private void serializeQuery(String query, ChannelBuffer buf) {
    	int size = query.getBytes().length;
    	buf.writerIndex(4);
        buf.writeInt(size);
        buf.writeBytes(query.getBytes());
        buf.writeInt(bindValues.size());
        for (int i = 0; i < bindValues.size(); i++) {
        	BindValue value = bindValues.get(i);
        	if (value.value == null) {
        		buf.writeByte(0);
        	} else {
        		buf.writeByte((byte) 1);
        		buf.writeInt(value.value.toString().getBytes().length);
        		buf.writeBytes(value.value.toString().getBytes());        		
        	}
        }
        buf.writeInt(0);
        int s = buf.writerIndex();
        buf.clear();
        buf.writeInt(s-4);
        buf.writerIndex(s);
	}
	
	public ResultSet executeQuery(ChannelBuffer buf) throws SQLException {
		connection.data.clear();
		connection.messageReceived = false;
    	connection.channel = connection.future.getChannel();
    	connection.tmpBuffer.clear();

    	ChannelFuture future = connection.channel.write(buf);
    	try {
			future.await();
		} catch (InterruptedException e1) {
		}
    	WireData data = null;
        while (true) {
        	try {
        		data = connection.data.take();
        		break;
        	} catch (InterruptedException e) {
        	}
        }
        
        if (data.buffer.readableBytes() == 0) {
        	throw new SQLException("InvalidSyntax", "", 5);
        }
        int rscolumns = data.buffer.readInt();
        List<org.wonderdb.query.sql.DreamDBResultSetMetadata.ResultSetColumn> list = new ArrayList<ResultSetColumn>();
        
        for (int i = 0; i < rscolumns; i++) {
        	int colSize = data.buffer.readInt();
        	byte[] bytes = new byte[colSize];
        	data.buffer.readBytes(bytes);
        	ResultSetColumn rsc = new ResultSetColumn();
        	rsc.type = data.buffer.readInt();
        	rsc.columnName = new String(bytes);
        	list.add(rsc);
        }
        metadata = new DreamDBResultSetMetadata(list);
        List<ResultSetValue> rsvList = new ArrayList<DreamDBPreparedStatement.ResultSetValue>();
//        int count = 0;
        while (true) {
        	while (true) {
	        	try {
	        		data = connection.data.take();
	        		break;
	        	} catch (InterruptedException e) {
	        	}
        	}
        	if (data.done) {
//        		count = data.buffer.readInt();
        		break;
        	}
    		ResultSetValue rsv = new ResultSetValue();
    		rsvList.add(rsv);
    		for (int i = 0; i < metadata.getColumnCount(); i++) {
    			ResultSetColumn rsc = metadata.metadata.get(i);
    			byte isNull = data.buffer.readByte();
    			if (isNull > 0) {
    				rsv.valueList.add(null);
    			} else {
        			switch (rsc.type) {
	        			case Types.VARCHAR: {
	        				int s = data.buffer.readInt();
	        				byte[] bytes = new byte[s];
	        				data.buffer.readBytes(bytes);
	        				rsv.valueList.add(new String(bytes));
	        				break;
	        			}
	        			case Types.NUMERIC: {
	        				long l = data.buffer.readLong();
	        				rsv.valueList.add(l);
	        				break;
	        			}
	        			case Types.DOUBLE:
	        				double d = data.buffer.readDouble();
	        				rsv.valueList.add(d);
	        				break;
						case Types.INTEGER:
							int val = data.buffer.readInt();
							rsv.valueList.add(val);
							break;
						case Types.FLOAT:
							float v = data.buffer.readFloat();
							rsv.valueList.add(v);
							break;
        			}
    			}        		
    		}
        }
        
        bindValues.clear();
        return new DreamDBResultSet(connection, this, metadata, rsvList);		
	}
	
	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
    	ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
    	serializeQuery(sql, buf);
    	return executeQuery(buf);
	}

	public int executeUpdate(ChannelBuffer buf) throws SQLException {
        int errCode = -1;
        String message = "";

		connection.data.clear();
		connection.messageReceived = false;
    	connection.tmpBuffer.clear();
    	connection.channel = connection.future.getChannel();
        ChannelFuture future = connection.channel.write(buf);
        future.awaitUninterruptibly();
    	WireData data = null;
        
    	while (true) {
        	try {
        		data = connection.data.take();
        		if (data.buffer == null) {
        			continue;
        		}
        		break;
        	} catch (InterruptedException e) {
        	}
        }
        
    	if (data.buffer.readableBytes() == 0) {
    		throw new SQLException("Invalid syntax", "", 5);
    	}
        updateCount = data.buffer.readInt();
        errCode = data.buffer.readInt();
        int msgSize = data.buffer.readableBytes();
        byte[] bytes = new byte[msgSize];
        data.buffer.readBytes(bytes);
        message = new String(bytes);
        bindValues.clear();
        if (errCode	> 0) {
        	throw new SQLException(message, "", errCode);
        }
        return updateCount;
		
	}
	
	@Override
	public int executeUpdate(String sql) throws SQLException {
    	ChannelBuffer buf = ChannelBuffers.dynamicBuffer(2048);
    	serializeQuery(sql, buf);
    	return executeUpdate(buf);
	}

	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return 0;
	}

	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getMaxRows() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean getMoreResults(int arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return updateCount;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	public void setCursorName(String arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setEscapeProcessing(boolean arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setMaxFieldSize(int arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setMaxRows(int arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setPoolable(boolean arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setQueryTimeout(int arg0) throws SQLException {
		throw new RuntimeException("Method not supported");
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
	public void addBatch() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void clearParameters() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean execute() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		// TODO Auto-generated method stub
		return executeQuery(sql);
	}

	@Override
	public int executeUpdate() throws SQLException {
		return executeUpdate(sql);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		bindValues.add(new BindValue(Types.VARBINARY, x));
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		bindValues.add(new BindValue(Types.DATE, x.getTime()));
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		bindValues.add(new BindValue(Types.DOUBLE, x));
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		bindValues.add(new BindValue(Types.FLOAT, x));
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		bindValues.add(new BindValue(Types.INTEGER, x));
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		bindValues.add(new BindValue(Types.NUMERIC, x));
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setNString(int parameterIndex, String value)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		bindValues.add(new BindValue(sqlType, null));
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		bindValues.add(new BindValue(Types.INTEGER, x));
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		bindValues.add(new BindValue(Types.VARCHAR, x));
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		bindValues.add(new BindValue(Types.TIME, x.getTime()));
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) {
		throw new RuntimeException("Method not supported");		
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		bindValues.add(new BindValue(Types.TIMESTAMP, x.getTime()));
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		throw new RuntimeException("Method not supported");		
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}
	
	private static class BindValue {
		private int type;
		private Object value;
		
		private BindValue(int type, Object value) {
			this.type = type;
			this.value = value;
		}
	}
	
	static class ResultSetValue {
		List<Object> valueList = new ArrayList<Object>();
	}
}
