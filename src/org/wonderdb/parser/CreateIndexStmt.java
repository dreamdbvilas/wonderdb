package org.wonderdb.parser;


import java.util.List;

public class CreateIndexStmt {
	public String table;
	public boolean unique;
	public List<IndexCol> colList;
	public String idxName;
	public String storage;
	public String toString() { return "Index Name: " + idxName + " \nTable: " + table + "\n Unique: " + unique + "\n Col List: " + colList + "\n storage: " + storage; }
}

