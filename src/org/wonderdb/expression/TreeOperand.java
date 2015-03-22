package org.wonderdb.expression;

import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexKeyType;
import org.wonderdb.types.TypeMetadata;
import org.wonderdb.types.record.TableRecord;

public class TreeOperand implements Operand {
	SimpleNode node = null;
	
	public TreeOperand() {
	}
	
	@Override
	public DBType getValue(IndexKeyType value, TypeMetadata meta) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType getValue(TableRecord value, TypeMetadata meta) {
		throw new RuntimeException("Method not supported");
	}

}
