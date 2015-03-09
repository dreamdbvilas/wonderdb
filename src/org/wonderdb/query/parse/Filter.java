package org.wonderdb.query.parse;


import java.util.List;

import org.wonderdb.expression.Expression;

public class Filter {
	public List<Expression> andList;
	public String toString() { return " And Filter: " + andList; }
}

