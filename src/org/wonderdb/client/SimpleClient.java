package org.wonderdb.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.wonderdb.client.jdbc.DreamDBConnection;


public class SimpleClient {
    static boolean messageReceived = false;
    static Object lock = new Object();
    
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        DreamDBConnection connection = new DreamDBConnection(host, port);
        
        while (true) {
        	System.out.print(host+":"+port+">");
        	String input = br.readLine();
//        	String input = "select * from employee";

        	if ("exit".equals(input)) {
	       		break;
	       	}
        	if (!input.isEmpty()) {	        		
		        
        		if (input.startsWith("help")) {
        			processHelp(connection);
        		}
        		if (input.startsWith("select")) {
		        	processSelectQuery(connection, input);
		        }
		        
		        if (input.startsWith("insert")) {
		        	processInsertQuery(connection, input);
		        }
		        
		        if (input.startsWith("update")) {
		        	processUpdateQuery(connection, input);
		        }
		        
		        if (input.startsWith("delete")) {
		        	processDeleteQuery(connection, input);
		        }
		        
		        if (input.startsWith("create table")) {
		        	processCreateTableQuery(connection, input);
		        }
		        
		        if (input.startsWith("create index") || input.startsWith("create unique index")) {
		        	processCreateIndexQuery(connection, input);
		        }
		        
		        if (input.startsWith("create storage")) {
		        	processCreateStorageQuery(connection, input);
		        }
		        
		        if (input.startsWith("create replicaset")) {
		        	processCreateReplicaSetQuery(connection, input);
		        }
		        
		        if (input.startsWith("create shard")) {
		        	processCreateShardQuery(connection, input);
		        }
		        
		        if (input.startsWith("add to replicaset")) {
		        	processAddToReplicaSetQuery(connection, input);
		        }
		        
		        if (input.startsWith("shutdown")) {
		        	processShutdownQuery(connection, input);
		        }
		        
		        if (input.startsWith("explain plan select")) {
		        	processSelectQuery(connection, input);
		        }
		        
		        if (input.startsWith("show")) {
		        	processSelectQuery(connection, input);
		        }
        	}
        	messageReceived = false;
        }        
    	connection.close();
    }
    
    private static void processHelp(Connection connection) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("SCHEMA COMMANDS\n");
    	sb.append("---------------\n");
    	sb.append("1. show schema\n\n");
    	sb.append("2. show storages\n\n");
    	sb.append("3. show table [table name]\n\n");
    	sb.append("4. create storage '<storage file name>' <block size multiples of 2048> is_default=<yes/no>\n\n");
    	sb.append("5. create table <table name> (<column name> <column type> [, <column name> <column type> ] [storage '<storage file name>']\n");
    	sb.append("column type: int, string, long, float, double\n\n");
    	sb.append("6. create index <index name> on <table name> (<column name> <column type> [, <column name>, <column type>] [storage '<storage file name>']\n");
    	sb.append("column type is required as you can create an index on column which is not defined yet\n\n");
    	
    	sb.append("DML COMMANDS\n");
    	sb.append("------------\n");
    	sb.append("7. insert into <table name> (<column name> [, <column name> ) values ( val1, val2 ...)\n\n");
    	sb.append("8. update <table name> set <column name> = val [, <column name> = val [where <filter>]\n\n");
    	sb.append("9. delete <table name> [where <filter>]\n\n");
    	sb.append("filter: For now filter can be made up of just and construct without any paranthisis, eg. empId = 1 and name like 'vilas'\n\n");
    	
    	sb.append("CLUSTER COMMANDS\n");
    	sb.append("----------------\n");
    	sb.append("For all clustering commnds, zookeeper and kafka server must be running\n");
    	sb.append("10. create replicaset <replicaset name>\n\n");
    	sb.append("11. add to replicaset <replicaset name> <nodeId>\n");
    	sb.append("12. nodeId can be found by opening nodeId file present where wonderdb server was started\n\n");
    	sb.append("13. create shard <table name> <replicaset name> <index name>\n");
    	sb.append("For this reference implementation, we are focusing mainly on replication aspect\n\n");
    	
    	sb.append("EXPLAIN PLAN\n");
    	sb.append("------------\n");
    	sb.append("explain plan <select query>\n\n");
    	System.out.println(sb.toString());
    }
    
    
    public static void processInsertQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
    	int count = 0;
    	try {
    		count = stmt.executeUpdate();
        	System.out.println(count + " record(s) inserted");
    	} catch (SQLException e) {
    		String message = e.getMessage();
    		System.out.println(message);
    	}
    }
    
    public static void processUpdateQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
    	int count = 0;
    	try {
    		count = stmt.executeUpdate();
        	System.out.println(count + " record(s) updated");
    	} catch (SQLException e) {
    		System.out.println(e.getMessage());
    	}
    }
    
    public static void processDeleteQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
    	int count = stmt.executeUpdate();
    	System.out.println(count + " record(s) deleted");
    }    
    
    public static void processCreateTableQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
		try {
			stmt.executeUpdate();
	    	System.out.println("table created ...");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }
    
    public static void processCreateIndexQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
    	try {
			stmt.executeUpdate();
	    	System.out.println("index created ...");
    	} catch (SQLException e) {
    		System.out.println(e.getMessage());
    	}
    }
    
    public static void processCreateStorageQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
		try {
			stmt.executeUpdate();
	    	System.out.println("storage created ...");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }
    
    public static void processCreateReplicaSetQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
		try {
			stmt.executeUpdate();
	    	System.out.println("replicaset created ...");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }
    
    public static void processCreateShardQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
		try {
			stmt.executeUpdate();
	    	System.out.println("replicaset created ...");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }
    
    public static void processAddToReplicaSetQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
		try {
			stmt.executeUpdate();
	    	System.out.println("added to replicaset ...");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }
    
    public static void processShutdownQuery(Connection connection, String sql) throws SQLException {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
    	stmt.executeUpdate();
    	System.out.println("shutdown complete");
    }
    
    public static void processSelectQuery(Connection connection, String sql) throws SQLException  {
		PreparedStatement stmt = null;
		stmt = connection.prepareStatement(sql);
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			return;
		}
		ResultSetMetaData rsm = rs.getMetaData();
		int count = 0;
		while (rs.next()) {
			count++;
			for (int i = 0; i < rsm.getColumnCount(); i++) {
				String colName = rsm.getColumnName(i);
				Object val = null;
				switch (rsm.getColumnType(i)) {
					case Types.NUMERIC:
						val = rs.getLong(i);
						break;
					case Types.VARCHAR:
						val = rs.getString(i);
						break;
					case Types.DOUBLE:
						val = rs.getDouble(i);
						break;
					case Types.INTEGER:
						val = rs.getInt(i);
						break;
					case Types.FLOAT:
						val = rs.getFloat(i);
						break;
				}
				if (colName == null || colName.length() == 0) {
					System.out.println(val);					
				} else {
					System.out.println(colName + "\t" + val);
				}
			}
			System.out.println();
		}
		System.out.println(count + " record(s) selected");
    }
    
    public static class SimpleClientHandler extends SimpleChannelHandler {
		ChannelBuffer tmpBuffer = ChannelBuffers.dynamicBuffer();
	
		int count = -1;
		byte endRec = 0;
		
		@Override
    	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
//    		int count = -1;
    		ChannelBuffer buf = (ChannelBuffer) e.getMessage();
    		tmpBuffer.writeBytes(buf, buf.readableBytes());
    		buf.clear();
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
//	    			int tmpBytes = tmpBuffer.readableBytes();
//	    			tmpBuffer.writeBytes(buf, buf.readerIndex(), count-tmpBuffer.readableBytes());
//	    			buf.readerIndex(buf.readerIndex() + count - tmpBytes);
	    			byte[] bytes = new byte[count];
	    			tmpBuffer.readBytes(bytes);
	    			String s = new String (bytes);
		    		System.out.println(s);
//		    		tmpBuffer.clear();
		    		count = -1;
	    		} else {
//	    			tmpBuffer.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
//	    			buf.clear();
	    			break;
	    		}
	    		
	    		if (endRec != 0) {
		    		synchronized (lock) {
		    			System.out.println("done");
		    			messageReceived = true;
		    			lock.notifyAll();
					}
		    		break;
	    		}
//	    		buf.clear();
    		}
    		
			if (tmpBuffer.capacity() > 100000) {
				ChannelBuffer newTmpBuffer = ChannelBuffers.dynamicBuffer();
				newTmpBuffer.writeBytes(tmpBuffer);
				tmpBuffer = newTmpBuffer;
				System.out.println("changed buffers");
			}
//    		buf.clear();
    		int i = 0;
//    		count = i;
    	}
    	
    	@Override
    	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    		e.getCause().printStackTrace();
    		e.getChannel().close();
    	}
    }
    
    public static class BufferDecoder extends FrameDecoder {
    	int count = -1;
    	@Override
        protected Object decode(
                ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
                
        	if (buffer.readableBytes() >= 5) {
        		if (count < 0) {
            		buffer.resetReaderIndex();
            		count = buffer.readInt();
        		}
        	} else {
        		return null;
        	}
 
        	if (buffer.readableBytes() >= count + 1) {
    			buffer.clear();
    			buffer.writerIndex(buffer.capacity());
    			buffer.readerIndex(4);
    			count = -1;
    			return buffer.readBytes(buffer.readableBytes());
        	}
        	return null;
        }    	
    }

}

