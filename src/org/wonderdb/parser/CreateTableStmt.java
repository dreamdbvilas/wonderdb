package org.wonderdb.parser;


import java.util.List;

import org.wonderdb.types.ColumnNameMeta;

public class CreateTableStmt {
	public String tableName;
	public List<ColumnNameMeta> colList;
	public String storage;
	public String toString() { return "Table Name: " + tableName + "\n Col List: " + colList + "\n storage: " + storage; }
}

