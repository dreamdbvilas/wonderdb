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
package org.wonderdb.block.index.impl.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.collection.IndexResultContent;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.StaticOperand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.query.plan.CollectionAlias;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.query.plan.ExpressionEvaluator;
import org.wonderdb.schema.Index;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.ColumnType;
import org.wonderdb.types.impl.IndexKeyType;


public  class ExpressionFilterIndexQuery implements IndexQuery {
	List<BasicExpression> expList;
	DataContext context;
	Index idx;
	CollectionAlias ca;
	Map<ColumnType, List<BasicExpression>> expByIdxColumns = null;
	
	public ExpressionFilterIndexQuery(List<BasicExpression> expList, DataContext context, Index idx, CollectionAlias ca) {
		this.expList = expList;
		this.context = context;
		this.idx = idx;
		this.ca = ca;
		expByIdxColumns = separateByIndexColumn();
	}
	
	private Map<ColumnType, List<BasicExpression>> separateByIndexColumn() {
		Map<ColumnType, List<BasicExpression>> retVal = new HashMap<ColumnType, List<BasicExpression>>();
		Set<ColumnType> idxColSet = idx.getColumnNameList();
		for (int i = 0; i < expList.size(); i++) {
			BasicExpression exp = expList.get(i);
			Operand left = exp.getLeftOperand();
			Operand right = exp.getRightOperand();

			if (left instanceof VariableOperand && right instanceof VariableOperand) {
				VariableOperand vol = (VariableOperand) left;
				VariableOperand vor = (VariableOperand) right;
				if (vol.getCollectionAlias().equals(ca) && idxColSet.contains(vol.getColumnType())) {
					// this is the column. now right should be reachable.
					DBType val = context.getValue(vor.getCollectionAlias(), vor.getColumnType(), vor.getPath());
					if (val == null) {
						continue;
					}
					addExp(retVal, exp, vol.getColumnType());
					continue;
				}
				if (vor.getCollectionAlias().equals(ca) && idxColSet.contains(vor.getColumnType())) {
					// this is the column. now right should be reachable.
					DBType val = context.getValue(vol.getCollectionAlias(), vol.getColumnType(), vol.getPath());
					if (val == null) {
						continue;
					}
					addExp(retVal, exp, vor.getColumnType());
				}
			} else {
				if (left instanceof VariableOperand && right instanceof StaticOperand) {
					VariableOperand vo = (VariableOperand) left;
					if (vo.getCollectionAlias().equals(ca) && idxColSet.contains(vo.getColumnType())) {
						addExp(retVal, exp, vo.getColumnType());
						continue;
					}
				} 
				if (left instanceof StaticOperand && right instanceof VariableOperand) {
					VariableOperand vo = (VariableOperand) right;
					if (vo.getCollectionAlias().equals(ca) && idxColSet.contains(vo.getColumnType())) {
						addExp(retVal, exp, vo.getColumnType());
						continue;
					}					
				}
			}
		}
		return retVal;
	}
	
	private void addExp(Map<ColumnType, List<BasicExpression>> map, BasicExpression exp, ColumnType ct) {
		List<BasicExpression> l = map.get(ct);
		if (l == null) {
			l = new ArrayList<BasicExpression>();
			map.put(ct, l);
		}
		l.add(exp);
	}
	
	public int compareTo(DBType k) {
		IndexKeyType key = null;
		if (k instanceof IndexKeyType) {
			key = (IndexKeyType) k;
		}
		
		else {
			throw new RuntimeException("Invalid type" + k);
		}
//		IndexQueryObject iqo = new IndexQueryObject(idx, key);
		int schemaId = SchemaMetadata.getInstance().getIndex(idx.getIndexName()).getSchemaId();
		context.add(ca, new IndexResultContent(key, schemaId));
		
		Iterator<ColumnType> iter = idx.getColumnNameList().iterator();
		while (iter.hasNext()) {
			ColumnType idxColumn = iter.next();
			List<BasicExpression> l = expByIdxColumns.get(idxColumn);
			if (l == null) {
				continue;
			}
			int c = ExpressionEvaluator.getInstance().compareTo(l, context, ca);
			if (c != 0) {
				return c;
			}
		}
		return 0;
	}
}
