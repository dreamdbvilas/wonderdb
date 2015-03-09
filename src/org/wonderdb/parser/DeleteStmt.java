package org.wonderdb.parser;


public class DeleteStmt {
	public String table;
	public Filter filter;
	public String toString() { return " Table Name: " + table + "\n Filter: " + filter; }
}

