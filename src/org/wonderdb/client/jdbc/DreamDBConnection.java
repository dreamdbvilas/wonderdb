package org.wonderdb.client.jdbc;

import java.net.InetSocketAddress;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class DreamDBConnection implements Connection {
    ChannelFactory factory = null;
    ClientBootstrap bootstrap = null;
    Object lock = new Object();
    ChannelFuture future = null;
    boolean messageReceived = false;
    BlockingQueue<WireData> data = new ArrayBlockingQueue<WireData>(200);
    Channel channel = null;
    int queryType = -1;
	ChannelBuffer tmpBuffer = ChannelBuffers.dynamicBuffer();
    
    public DreamDBConnection(String host, int port) {
//    	BufferDecoder bufferDecoder = new BufferDecoder();
        factory =
            new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool());

        bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline  (/* new BufferDecoder(), */ 
                		new ConnectionDataHandler());
            }
        });
        
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        future = bootstrap.connect(new InetSocketAddress(host, port));
//    	channel = future.getChannel();
    	ChannelFutureListener listener = new ConnectListener();
    	future.addListener(listener);
		synchronized (future) {
	    	while (!future.isSuccess()) {
    			try {
    				future.wait();
    				break;
    			} catch (InterruptedException e) {
    			}
	    	}
		}
		future.removeListener(listener);
    }
    
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public void close() throws SQLException {		
		channel.close();
        channel.getCloseFuture().awaitUninterruptibly();
        factory.releaseExternalResources();
	}

	@Override
	public void commit() throws SQLException {
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new DreamDBPreparedStatement(this, null);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return false;
	}

	@Override
	public String getCatalog() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return null;
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		return 0;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return 0;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return false;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new DreamDBPreparedStatement(this, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void rollback() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new RuntimeException("Method not supported");
	}
	
	public class ConnectionDataHandler extends SimpleChannelHandler {
		
		int count = -1;
		byte endRec = 0;
		
		@Override
    	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    		ChannelBuffer buf = (ChannelBuffer) e.getMessage();
    		tmpBuffer.writeBytes(buf, buf.readableBytes());
    		while (tmpBuffer.readableBytes() > 0) {
	    		if (count < 0) {
	    			if (tmpBuffer.readableBytes() >= 5) {	    				
		    			count = tmpBuffer.readInt();
		    			endRec = tmpBuffer.readByte();
	    			} else {
	    				break;
	    			}
	    		}
	    		
	    		if (count <= tmpBuffer.readableBytes()) {
	    			byte[] bytes = new byte[count];
	    			tmpBuffer.readBytes(bytes);
	    			ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(bytes);
    				while (true) {
    	    			try {
	    					data.put(new WireData(endRec != 0 ? true: false, buffer));
	    					break;
	    				} catch (InterruptedException e1) {
	    					e1.printStackTrace();
	    				}
	    			}
		    		count = -1;
	    		} else {
	    			break;
	    		}
	    		
				if (tmpBuffer.capacity() > 1000) {
					ChannelBuffer newTmpBuffer = ChannelBuffers.dynamicBuffer();
					newTmpBuffer.writeBytes(tmpBuffer);
					tmpBuffer = newTmpBuffer;
				}

				if (endRec != 0) {
		    		synchronized (lock) {
			    		endRec = 0;
			    		tmpBuffer.clear();
		    			messageReceived = true;
		    			lock.notifyAll();
		    			while (true) {		    				
		    				try {
								data.put(new WireData(true, null));
			    				break;
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
		    			}
					}
		    		break;
	    		}
    		}
    	}
    	
    	@Override
    	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    		e.getCause().printStackTrace();
    		e.getChannel().close();
    	}
	}
	
	class WireData {
		boolean done = false;
		ChannelBuffer buffer = null;
	
		WireData(boolean done, ChannelBuffer buffer) {
			this.done = done;
			this.buffer = buffer;
		}
	}
	
	public class ConnectListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture arg0) throws Exception {
			synchronized (arg0) {
				arg0.notifyAll();
			}
		}
	}
	
    public class BufferDecoder extends FrameDecoder {
    
    	
    	@Override
        protected Object decode(
                ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
                
//        	if (buffer.capacity() >= 4) {
           	if (buffer.readableBytes() >= 4) {
        		buffer.resetReaderIndex();
        		int count = buffer.readInt();
        		buffer.resetReaderIndex();
        		if (buffer.readableBytes() < count) {
        			return null;
        		} else {
        			buffer.clear();
        			buffer.writerIndex(buffer.capacity());
        			buffer.readerIndex(4);
        			return buffer.readBytes(buffer.readableBytes());
        		}
        	}
        	return null;
        }    	
    }

	public static void main(String[] s) throws Exception {
		DriverManager.registerDriver(new DreamDBDriver());
		Connection  connection = DriverManager.getConnection("wonderdb://localhost:6060");
		Connection  connection1 = DriverManager.getConnection("wonderdb://localhost:6060");
		PreparedStatement stmt = null;
		Random random = new Random(System.currentTimeMillis());
//		stmt = connection.prepareStatement("create table employee (id long, name string)");
//		stmt = connection.prepareStatement("shutdown");
//		stmt.executeUpdate();
//		stmt = connection.prepareStatement("explain plan select * from employee where name like ?");
//		stmt = connection.prepareStatement("select i from allVars");
////		stmt.setString(1, "vi");
//		ResultSet rs = stmt.executeQuery();
//		ResultSetMetaData rsm = rs.getMetaData();
//		while (rs.next()) {
//			for (int i = 0; i < rsm.getColumnCount(); i++) {
//				String colName = rsm.getColumnName(i);
//				Object val = null;
//				switch (rsm.getColumnType(i)) {
//					case Types.NUMERIC:
//						val = rs.getLong(i);
//						break;
//					case Types.VARCHAR:
//						val = rs.getString(i);
//						break;
//					case Types.DOUBLE:
//						val = rs.getDouble(i);
//						break;
//				}
//				System.out.println(colName + "\t" + val);
//			}
//			System.out.println();
//		}
		
//		int start = Integer.parseInt(s[0]);
//		int end = Integer.parseInt(s[1]);
//		stmt = connection.prepareStatement("update employee set name = ? where id = ?");
	 	stmt = connection.prepareStatement("update employee set name = '132_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_132' where id >= 69109 and id > 69110");
//	 	PreparedStatement stmt1 = connection1.prepareStatement("update employee set name = ? where id = ?");
	 	
//		for (int i = start; i < end; i++) {
			StringBuilder builder = new StringBuilder();
			int maxVal = 5000; //Math.abs(random.nextInt()) % 2500;
			for (int x = 0; x < maxVal; x++) {
				builder.append("a");
			}
//			stmt = connection.prepareStatement("update employee set name = ? where id = ?");
//			stmt = connection.prepareStatement("select * from employee where id = ?");
//		 	stmt = connection.prepareStatement("insert into newStorage3 (name, id) values (?, ?)");
//			stmt.setString(1, i+"_"+builder.toString()+"_"+i);
//			stmt.setLong(2, i);
//			stmt.executeUpdate();
//			stmt.executeUpdate();
//			stmt1.setString(1, i+"_"+builder.toString()+"_"+i);
//			stmt1.setLong(2, i);
//			stmt1.executeUpdate();
//			ResultSet rs = stmt.executeQuery();
//			if (rs.next()) {
//				System.out.println(rs.getString(1));
//			} else {
//				System.out.println(i);
//				break;
//			}
//		}
//		connection.close();
//		connection1.close();
	}
}
