package org.wonderdb.expression;

import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parse.DBInsertQuery;
import org.wonderdb.query.parse.StaticOperand;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.record.TableRecord;

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
		if (left instanceof StaticOperand) {
			if (right instanceof VariableOperand) {
				CollectionAlias ca = ((VariableOperand) right).getCollectionAlias();
				int colId = ((VariableOperand) right).getColumnId();
				CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
				int type = colMeta.getCollectionColumn(colId).getColumnType();
				DBType val = DBInsertQuery.convertToDBType(left.getValue((TableRecord) null, null), type);
				this.left = new StaticOperand(val);
			}
		}

		if (right instanceof StaticOperand) {
			if (left instanceof VariableOperand) {
				CollectionAlias ca = ((VariableOperand) left).getCollectionAlias();
				int colId = ((VariableOperand) left).getColumnId();
				CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
				int type = colMeta.getCollectionColumn(colId).getColumnType();
				DBType val = DBInsertQuery.convertToDBType(right.getValue((TableRecord) null, null), type);
				this.right = new StaticOperand(val);
			}
		}
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
