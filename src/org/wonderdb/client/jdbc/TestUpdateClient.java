package org.wonderdb.client.jdbc;

import java.sql.PreparedStatement;
import java.util.Random;

public class TestUpdateClient {
	public static void main(String[] args) throws Exception {
		DreamDBConnection  connection = new DreamDBConnection("localhost", 6060);
		PreparedStatement stmt = null;
		Random random = new Random(System.currentTimeMillis());
//		int start = Integer.parseInt(args[0]);
		int end = Integer.parseInt(args[0]);
		int updateCount = Integer.parseInt(args[1]);
	 	stmt = connection.prepareStatement("update employee set name = ?, count = ? where id >= ? and id <= ?");
	 	
	 	String s = null;
	 	for (int i = 0; i < end; i++) {
			StringBuilder builder = new StringBuilder();
			int maxVal = Math.abs(random.nextInt()) % 100;
			for (int x = 0; x < maxVal; x++) {
				builder.append("a");
			}
			
			long us = Math.abs(random.nextInt()) % 100000;
			long usc = us + Math.abs(random.nextInt()) % updateCount;
			
//		 	stmt = connection.prepareStatement("insert into employee (name, id) values (?, ?)");
		 	s = i+"_"+builder.toString()+"_"+i;
			stmt.setString(1, s);
			stmt.setInt(2, s.length());
			stmt.setLong(3, us);
			stmt.setLong(4, usc);
			try {
				stmt.executeUpdate();
				System.out.println(i);
			} catch (Exception e) {
				System.out.println(s);
				System.out.println(us);
				System.out.println(usc);
			}
		}
		connection.close();
	}
}
