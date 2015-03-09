package org.wonderdb.parser;


public class Expression {
	public String leftOp;
	public String op;
	public String rightOp;
	public boolean likeOp = false;
	public String toString() { return "Left OP: " + leftOp + " op: " + op + " right op: " + rightOp + " like: " + likeOp; }
}

