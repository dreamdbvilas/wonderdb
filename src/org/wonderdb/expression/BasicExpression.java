/*******************************************************************************
 *    Copyright 2013 Vilas Athavale
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package org.wonderdb.expression;



public class BasicExpression implements Expression {
	private Operand left;
	private Operand right;
	private int operator;
	
	public BasicExpression(Operand left, Operand right, int operator) {
		if (left == null || right == null) {
			throw new RuntimeException("Invalid Expression: rightOperand = " + right + " LeftOperand = " + left);
		}
		this.left = left;
		this.right = right;
		this.operator = operator;
	}	
	
	public Operand getLeftOperand() {
		return left;
	}
	
	public Operand getRightOperand() {
		return right;
	}
	
	public int getOperator() {
		return operator;
	}
	
	public void setLeftOperand(Operand op) {
		left = op;
	}
	
	public void setRightOperand(Operand op) {
		right = op;
	}	
}
