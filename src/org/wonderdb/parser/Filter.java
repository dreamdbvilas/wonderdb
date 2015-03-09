package org.wonderdb.parser;


import java.util.List;

public class Filter {
	public List<Expression> andList;
	public String toString() { return " And Filter: " + andList; }
}

