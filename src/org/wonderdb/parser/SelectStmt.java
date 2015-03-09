package org.wonderdb.parser;


import java.util.List;


public class SelectStmt {
	public List<String> selectColList;
	public List<TableDef> tableDefList;
	public Filter filter;
	public String toString() { return "Select: " + selectColList + " \n TableDef list: " + tableDefList + " \n Filter: " + filter; }
}

