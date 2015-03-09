package org.wonderdb.parser;


import java.util.List;

public class InsertStmt {
	public String table;
	public List<String> columns;
	public InsertColumns values;
	public String toString() { return "Table: "+ table + "\n" + "Insert Columns: " + columns + "\n" + "Values: " + values + "\n"; }
}

