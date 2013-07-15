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
package org.wonderdb.query.plan;

import java.util.List;

import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Expression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.StaticOperand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.StringLikeType;
import org.wonderdb.types.impl.StringType;


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
				leftVal = dataContext.getValue(vo.getCollectionAlias(), vo.getColumnType(), vo.getPath());
				if (vo.getCollectionAlias().equals(ca)) {
					invert = true;
				}
			} else {
				leftVal = ((StaticOperand) left).getValue(null);
			}
			
			if (right instanceof VariableOperand) {
				VariableOperand vo = (VariableOperand) right;
				rightVal = dataContext.getValue(vo.getCollectionAlias(), vo.getColumnType(), vo.getPath());
			} else {
				rightVal = ((StaticOperand) right).getValue(null);
			}
			
			c = evaluate(leftVal, rightVal, exp);
			if (invert) {
				c = c*-1;
			}
			if (c != 0) {
				break;
			}
		}
		return c;
	}
	
	public int evaluate(DBType left, DBType right, BasicExpression exp) {
		switch (exp.getOperator()) {
		case Expression.EQ:
			return evaluateEQOrLike(left, right, exp);
		case Expression.LIKE:
			if (right instanceof StringType) {
				right = new StringLikeType(((StringType) right).get());
			}
			return evaluateEQOrLike(left, right, exp);
		case Expression.GE:
			return evaluateGE(left, right, exp);
		case Expression.GT:
			return evaluateGT(left, right, exp);
		case Expression.LE:
			return evaluateLE(left, right, exp);
		case Expression.LT:
			return evaluateLT(left, right, exp);
			
		}
		return -1;
	}
	
	private int evaluateEQOrLike(DBType left, DBType right, BasicExpression exp) {
		if (left == null && right == null) {
			return 0;
		} 
		
		if (left == null) {
			return -1;
		}
		return left.compareTo(right);
	}
	
	private int evaluateGE(DBType left, DBType right, BasicExpression exp) {
		if (left == null) {
			if (right == null) {
				return 0;
			} else {
				return -1;
			}
		}
		return left.compareTo(right) >= 0 ? 0 : -1;		
	}

	private int evaluateGT(DBType left, DBType right, BasicExpression exp) {
		if (left == null) {
			return -1;
		}
		return left.compareTo(right) > 0 ? 0 : -1;
	}

	private int evaluateLE(DBType left, DBType right, BasicExpression exp) {
		if (left == null) {
			return 0;
		}
		return left.compareTo(right) <= 0 ? 0 : 1;
	}

	private int evaluateLT(DBType left, DBType right, BasicExpression exp) {
		if (left == null) {
//			if (right == null) {
				return -1;
//			}
//			return 0;
		}
		return left.compareTo(right) < 0 ? 0 : 1;
	}
}
