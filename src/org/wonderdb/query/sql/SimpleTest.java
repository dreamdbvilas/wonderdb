package org.wonderdb.query.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SimpleTest {
	public static void main(String[] args) throws SQLException {
		DriverManager.registerDriver(new WonderDBDriver());
		Connection con = DriverManager.getConnection("wonderdb://localhost:6060");
//		String sql = "select * from vilas where a > ?;";
		String sql = "insert into vilas (id, a, b) values (0, ?, ?);";
		PreparedStatement stmt = con.prepareStatement(sql);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1500; i++) {
			sb.append("a");
		}
		for (int i = 0; i < 1; i++) {
//			stmt.setInt(0, i);
			stmt.setString(0, "" + i + sb.toString());
			stmt.setString(1, "" + i + sb.toString());
			
	//		stmt.setInt(0, 0);
			stmt.executeUpdate();
			System.out.println(i);
		}
//		ResultSet rs = stmt.executeQuery();
//		while (rs.next()) {
//			int i = rs.getInt(0);
//			String s = rs.getString(1);
//			int x = 0;
//			System.out.println(i + " " + s);
//		}
	}
}
