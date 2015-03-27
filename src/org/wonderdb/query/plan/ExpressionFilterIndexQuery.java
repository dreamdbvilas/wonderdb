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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wonderdb.block.IndexQuery;
import org.wonderdb.collection.IndexResultContent;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parse.StaticOperand;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.IndexRecord;



public  class ExpressionFilterIndexQuery implements IndexQuery, Comparator<DBType> {
	List<BasicExpression> expList;
	DataContext context;
	IndexNameMeta idx;
	CollectionAlias ca;
	Map<Integer, List<BasicExpression>> expByIdxColumns = null;
	Set<Object> pinnedBlocks = null;
	
	public ExpressionFilterIndexQuery(List<BasicExpression> expList, DataContext context, IndexNameMeta idx, CollectionAlias ca, Set<Object> pinnedBlocks) {
		this.expList = expList;
		this.context = context;
		this.idx = idx;
		this.ca = ca;
		expByIdxColumns = separateByIndexColumn();
		this.pinnedBlocks = pinnedBlocks;
	}
	
	private Map<Integer, List<BasicExpression>> separateByIndexColumn() {
		Map<Integer, List<BasicExpression>> retVal = new HashMap<Integer, List<BasicExpression>>();
		List<Integer> idxColSet = idx.getColumnIdList();
		for (int i = 0; i < expList.size(); i++) {
			BasicExpression exp = expList.get(i);
			Operand left = exp.getLeftOperand();
			Operand right = exp.getRightOperand();

			if (left instanceof VariableOperand && right instanceof VariableOperand) {
				VariableOperand vol = (VariableOperand) left;
				VariableOperand vor = (VariableOperand) right;
				if (vol.getCollectionAlias().equals(ca) && idxColSet.contains(vol.getColumnId())) {
					// this is the column. now right should be reachable.
					DBType val = context.getValue(vor.getCollectionAlias(), vor.getColumnId(), null);
					if (val == null) {
						continue;
					}
					addExp(retVal, exp, vol.getColumnId());
					continue;
				}
				if (vor.getCollectionAlias().equals(ca) && idxColSet.contains(vor.getColumnId())) {
					// this is the column. now right should be reachable.
					DBType val = context.getValue(vol.getCollectionAlias(), vol.getColumnId(), null);
					if (val == null) {
						continue;
					}
					addExp(retVal, exp, vor.getColumnId());
				}
			} else {
				if (left instanceof VariableOperand && right instanceof StaticOperand) {
					VariableOperand vo = (VariableOperand) left;
					if (vo.getCollectionAlias().equals(ca) && idxColSet.contains(vo.getColumnId())) {
						addExp(retVal, exp, vo.getColumnId());
						continue;
					}
				} 
				if (left instanceof StaticOperand && right instanceof VariableOperand) {
					VariableOperand vo = (VariableOperand) right;
					if (vo.getCollectionAlias().equals(ca) && idxColSet.contains(vo.getColumnId())) {
						addExp(retVal, exp, vo.getColumnId());
						continue;
					}					
				}
			}
		}
		return retVal;
	}
	
	private void addExp(Map<Integer, List<BasicExpression>> map, BasicExpression exp, Integer ct) {
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
		} else if (k instanceof IndexRecord) {
			DBType c = ((IndexRecord) k).getColumn();
			if (c instanceof IndexKeyType) {
				key = (IndexKeyType) c;
			}
		} else {
			throw new RuntimeException("Invalid type" + k);
		}
//		IndexQueryObject iqo = new IndexQueryObject(idx, key);
		TypeMetadata meta = SchemaMetadata.getInstance().getIndexMetadata(idx);
		context.add(ca, new IndexResultContent(key, meta, pinnedBlocks));
		
		Iterator<Integer> iter = idx.getColumnIdList().iterator();
		while (iter.hasNext()) {
			int idxColumn = iter.next();
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
	
	@Override
	public int hashCode() {
		throw new RuntimeException("Method not supported");
	}
	
	@Override
	public boolean equals(Object o) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType copyOf() {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public Comparator<DBType> getComparator() {
		return this;
	}

	@Override
	public int compare(DBType o1, DBType o2) {
		if (o1 instanceof ExpressionFilterIndexQuery) {
			return compareTo(o2);
		} else if (o2 instanceof ExpressionFilterIndexQuery) {
			return compareTo(o1);
		}
		throw new RuntimeException("Method not supported");
	}
}
