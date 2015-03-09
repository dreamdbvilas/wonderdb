package org.wonderdb.parser;


import java.util.List;

public class InsertColumns {
	public List<String> list=null;
	public SelectStmt stmt = null;
	public String toString() { String s = stmt != null ? " Select Stmt: " + stmt.toString() : ""; return "Insert Columns: " + list + s; }
}

