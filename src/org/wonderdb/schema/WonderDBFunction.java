package org.wonderdb.schema;


import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.types.DBType;

public interface WonderDBFunction {
	public DBType process(DataContext context);
	public boolean isAggregate();
	public DBType getDBType();
	public WonderDBFunction newInstance(SimpleNode node);
	public Integer getColumnType();
}
