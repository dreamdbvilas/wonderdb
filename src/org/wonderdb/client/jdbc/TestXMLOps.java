package org.wonderdb.client.jdbc;

import java.sql.PreparedStatement;

public class TestXMLOps {

	static String xml = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>" +
						"<bookstore>" +
						"	<book category=\"COOKING\">" +
						"		<title lang=\"en\">Everyday Italian</title>" +
						"		<author>Giada De Laurentiis</author>" +
						"		<year>2005</year>" +
						"		<price>30.00</price>" +
						"	</book>" +
						"	<book category=\"CHILDREN\">" +
						"		<title lang=\"en\">Harry Potter</title>" +
						"		<author>J K. Rowling</author>" +
						"		<year>2005</year>" +
						"		<price>29.99</price>" +
						"	</book>" +
						"	<book category=\"WEB\">" +
						"		<title lang=\"en\">XQuery Kick Start</title>" +
						"		<author>James McGovern</author>" +
						"		<author>Per Bothner</author>" +
						"		<author>Kurt Cagle</author>" +
						"		<author>Ja" +
						"mes Linn</author>" +
						"		<author>Vaidyanathan Nagarajan</author>" +
						"		<year>2003</year>" +
						"		<price>49.99</price>" +
						"	</book>" +
						"	<book category=\"WEB\">" +
						"		<title lang=\"en\">Learning XML</title>" +
						"		<author>Erik T. Ray</author>" +
						"		<year>2003</year>" +
						"		<price>39.95</price>" +
						"	</book>" +
						"</bookstore>";
	
	public static void main (String[] args) throws Exception {
		DreamDBConnection  connection = new DreamDBConnection("localhost", 6060);
		PreparedStatement stmt = null;
	 	stmt = connection.prepareStatement("insert into bookstore (id, name, data) values (?, ?, ?)");
		stmt.setLong(1, 1);
		stmt.setString(2, "vilas's");
		stmt.setString(3, xml);
		stmt.executeUpdate();
		connection.close();
	}
}
