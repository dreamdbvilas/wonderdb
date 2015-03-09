package org.wonderdb.parser;


import java.util.List;

public class UpdateStmt {
	public String table;
	public List<UpdateSetExpression> updateSetExpList;
	public Filter filter;
	public String toString() { return "Table: " + table + "\n Update Set: " + updateSetExpList + "\n Filter: " + filter; }
}

