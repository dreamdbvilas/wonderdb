package org.wonderdb.query.plan;

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

import java.util.List;

import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Expression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parse.StaticOperand;
import org.wonderdb.types.DBType;
import org.wonderdb.types.StringLikeType;
import org.wonderdb.types.StringType;



public class ExpressionEvaluator {
	private static ExpressionEvaluator instance = new ExpressionEvaluator();
	
	private ExpressionEvaluator() {
	}
	
	public static ExpressionEvaluator getInstance() {
		return instance;
	}
	
	public int compareTo(List<BasicExpression> expList, DataContext dataContext, CollectionAlias ca) {
		int c = -1;
		for (int i = 0; i < expList.size(); i++) {
			BasicExpression exp = (BasicExpression) expList.get(i);
			Operand left = exp.getLeftOperand();
			Operand right = exp.getRightOperand();
			DBType leftVal = null;
			DBType rightVal = null;
			boolean invert = false;
			
			if (left instanceof VariableOperand) {
				VariableOperand vo = (VariableOperand) left;
				leftVal = dataContext.getValue(vo.getCollectionAlias(), vo.getColumnId(), vo.getPath());
			} else {
				invert = true;
				leftVal = ((StaticOperand) left).getValue(null, null);
			}
			
			if (right instanceof VariableOperand) {
				VariableOperand vo = (VariableOperand) right;
				rightVal = dataContext.getValue(vo.getCollectionAlias(), vo.getColumnId(), vo.getPath());
				invert = true;
			} else {
				invert = false;
				rightVal = ((StaticOperand) right).getValue(null, null);
			}
			
			int op = exp.getOperator();
			
			op = mapOp(invert, op);
			if (invert) {
				c = evaluate(rightVal, leftVal, op);
			} else {
				c = evaluate(leftVal, rightVal, op);				
			}
//			if (invert) {
//				c = c*-1;
//			}
			if (c != 0) {
				break;
			}
		}
		return c;
	}
	
	private int mapOp(boolean invert, int op) {
		switch (op) {
		case Expression.EQ:
			return Expression.GE;
		case Expression.GE:
			if (!invert) {
				return Expression.GE;
			} 
			return Expression.LE;
		case Expression.LE:
			if (!invert) {
				return Expression.LE;
			}
			return Expression.GE;
		case Expression.LT:
			if (!invert) {
				return Expression.LT;
			}
			return Expression.GT;
		}
		return op;
	}
	
	public int evaluate(DBType left, DBType right, int op) {
		switch (op) {
		case Expression.EQ:
			return evaluateEQOrLike(left, right);
		case Expression.LIKE:
			if (right instanceof StringType) {
				right = new StringLikeType(((StringType) right).get());
			}
			return evaluateEQOrLike(left, right);
		case Expression.GE:
			return evaluateGE(left, right);
		case Expression.GT:
			return evaluateGT(left, right);
		case Expression.LE:
			return evaluateLE(left, right);
		case Expression.LT:
			return evaluateLT(left, right);
			
		}
		return -1;
	}
	
	private int evaluateEQOrLike(DBType left, DBType right) {
		if (left == null && right == null) {
			return 0;
		} 
		
		if (left == null) {
			return -1;
		}
		int c = left.compareTo(right);
		return c <= 0 ? 1 : -1;
	}
	
	private int evaluateGE(DBType left, DBType right) {
		if (left == null) { 
			if (right == null) {
				return 0;
			} else {
				return -1;
			}
		}
		
		int c = left.compareTo(right);
		return c >= 0 ? 1 : -1;		
	}

	private int evaluateGT(DBType left, DBType right) {
		if (left == null) {
			return -1;
		}
		int c = left.compareTo(right);
		return c > 0 ? 1 : -1;
	}

	private int evaluateLE(DBType left, DBType right) {
		if (left == null) {
			return 0;
		}
		return 1;
//		int c = left.compareTo(right);
//		return c < 0 ? -1 : 1;
	}

	private int evaluateLT(DBType left, DBType right) {
		if (left == null) {
				return 0;
		}
		return 1;
//		int c = left.compareTo(right);
//		return c < 0 ? 1 : -1;
	}
}
