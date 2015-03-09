package org.wonderdb.parser;

import org.wonderdb.expression.VariableOperand;
import org.wonderdb.parser.jtree.SimpleNode;


public class UpdateSetExpression {
	public VariableOperand column;
	public SimpleNode value;
	public String toString() { return " Column: " + column + " Value: " + value; }
}

