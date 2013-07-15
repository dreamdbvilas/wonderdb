package org.wonderdb.client.jdbc;

import java.sql.PreparedStatement;
import java.util.Random;

public class TestInsertClient {
	public static void main(String[] args) throws Exception {
		DreamDBConnection  connection = new DreamDBConnection("localhost", 6060);
		PreparedStatement stmt = null;
		Random random = new Random(System.currentTimeMillis());
		int start = Integer.parseInt(args[0]);
		int end = Integer.parseInt(args[1]);
	 	stmt = connection.prepareStatement("insert into employee (name, id, count) values (?, ?, ?)");
	 	
	 	String s = null;
		System.out.println(start);
		System.out.println(end);
	 	for (int i = start; i < end; i++) {
			StringBuilder builder = new StringBuilder();
			int maxVal = Math.abs(random.nextInt()) % 50;
			for (int x = 0; x < maxVal; x++) {
				builder.append("a");
			}
//		 	stmt = connection.prepareStatement("insert into employee (name, id) values (?, ?)");
		 	s = i+"_"+builder.toString()+"_"+i;
			stmt.setString(1, s);
			stmt.setLong(2, i);
			stmt.setInt(3, s.length());
			stmt.executeUpdate();
		}
		connection.close();
	}
}
