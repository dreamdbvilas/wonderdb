package org.wonderdb.parser.jtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.wonderdb.expression.AndExpression;
import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parse.StaticOperand;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree.ResultSetColumn;
import org.wonderdb.query.parser.jtree.DBSelectQueryJTree.VirtualResultSetColumn;
import org.wonderdb.query.plan.DataContext;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.FunctionManager;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.schema.WonderDBFunction;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.DBType;
import org.wonderdb.types.DoubleType;
import org.wonderdb.types.FloatType;
import org.wonderdb.types.IntType;
import org.wonderdb.types.LongType;
import org.wonderdb.types.StringType;
import org.wonderdb.types.UndefinedType;


public class QueryEvaluator {
	DataContext context = null;
//	CollectionAlias ca = null;
	Map<String, CollectionAlias> aliasMap = new HashMap<>();
	
	public QueryEvaluator(Map<String, CollectionAlias> aliasMap, DataContext dataContext) {
		this.context = dataContext;
		this.aliasMap = aliasMap;
//		this.ca = ca;
//		this.fromMap = fromMap;
	}

	public void evaluate(SimpleNode node) {
		SimpleNode c = (SimpleNode) node.children[0];
		switch (c.id) {
		case UQLParserTreeConstants.JJTSELECTSTMT:
			processSelectStmt(c);
		}
	}
	
	public void shouldQueryRewrite(SimpleNode start) {
		SimpleNode selectNode = (SimpleNode) start.children[0];
		SimpleNode compareNode = null;
		SimpleNode node = null;
		
		for (int i = 0; i < selectNode.children.length; i++) {
			compareNode = (SimpleNode) selectNode.children[i];
			if (compareNode.id == UQLParserTreeConstants.JJTFILTEREXPRESSION) {
				break;
			}			
		}
		
		if (compareNode.id != UQLParserTreeConstants.JJTFILTEREXPRESSION) {
			return;
		}
		
		List<SimpleNode> list = new ArrayList<SimpleNode>();
		List<SimpleNode> endList = new ArrayList<SimpleNode>();
		node = compareNode;
		boolean foundOr = false;
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			if (n.id == UQLParserTreeConstants.JJTOR) {
				endList.add(n);
				foundOr = true;
			} else if (foundOr) {
				endList.add(n);
				foundOr = false;
			} else {
				list.add(n);
			}
		}		
		node.children = new SimpleNode[list.size()+endList.size()];
		for (int i = 0; i < list.size(); i++) {
			node.children[i] = list.get(i);
		}
		
		for (int i = 0; i < endList.size(); i++) {
			node.children[i+list.size()] = endList.get(i);
		}
	}
	
	public boolean processSelectStmt(SimpleNode startNode) {
		SimpleNode node = startNode;
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			switch (n.id) {
			case UQLParserTreeConstants.JJTFILTEREXPRESSION:
				processFilterExpression(n);
				Object val = n.jjtGetValue();
				if (val instanceof Boolean) {
					return (Boolean) val;
				} else if (val instanceof UndefinedType) {
					return true;
				} else {
					throw new RuntimeException("Invalid syntax");
				}
			default:
				break;
			}
		}
		return true;
	}
	
	private void processFilterExpression(SimpleNode node) {
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			switch (n.id) {
			case UQLParserTreeConstants.JJTCOMPAREEQUATION:
				processCompareEquation(n);
				break;
			default:
				break;
			}
		}
		
		node.jjtSetValue(((SimpleNode) node.children[0]).jjtGetValue()); 
	}
	
	private void processCompareEquation(SimpleNode node) {
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			switch (n.id) {
			case UQLParserTreeConstants.JJTEQUALITYEQUATION:
				processEqualityEquation(n);
				break;
			default:
				break;
			}
		}
		
		evaluateCompareEquation(node);
	}
	
	private void evaluateCompareEquation(SimpleNode node) {
		boolean currentFlag = true;
		int op = -1;
		
		if (node.children.length == 1) {
			node.jjtSetValue( ((SimpleNode) node.children[0]).jjtGetValue());
			return;
		}
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			boolean forBreak = false;
			switch (n.id) {
			case UQLParserTreeConstants.JJTEQUALITYEQUATION: 
				currentFlag = evaluateAndOr(op, currentFlag, n.jjtGetValue());
				if (op == UQLParserTreeConstants.JJTOR && currentFlag) {
					forBreak = true;
					break;
				}
				break;
			case UQLParserTreeConstants.JJTAND:
			case UQLParserTreeConstants.JJTOR:
				op = n.id;
				break;
			}
			if (forBreak) {
				break;
			}
		}
		node.jjtSetValue(currentFlag);		
	}
	
	private boolean evaluateAndOr(int op, boolean currentFlag, Object val) {
		if (op < 0 && val != null) {
			if (val instanceof Boolean) {
				return (Boolean) val;
			} else {
				return true;
			}
		} else if (op < 0 && val == null) {
			throw new RuntimeException("Invalid expression");
		}
		
		boolean b = false;
		
		if (val instanceof Boolean) {
			b = (Boolean) val;
		} 

		switch (op) {
		case UQLParserTreeConstants.JJTAND:
			return val instanceof UndefinedType ? currentFlag : currentFlag && b;
		case UQLParserTreeConstants.JJTOR:
			return val instanceof UndefinedType ? currentFlag : currentFlag || b;
		}
		
		return false;
	}
	
	private void processEqualityEquation(SimpleNode node) {
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			switch(n.id) {
			case UQLParserTreeConstants.JJTMULTIPLICATIVEEXPRESSION:
				processMultiplicativeExpression(n);
				break;
			default:
				break;
			}
		}
		
		evaluateEqualityEquation(node);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void evaluateEqualityEquation(SimpleNode node) {
		
		Comparable left = null;
		Comparable right = null;
		int op = -1;
		
		Object o1 = (( SimpleNode) node.children[0]).jjtGetValue();
		Object o2 = null;
		
		if (node.children.length == 1) {
			node.jjtSetValue(o1);
			return;
		}
		
		if (node.children.length == 3) {
			op = ((SimpleNode) node.children[1]).id;
			o2 = (( SimpleNode) node.children[2]).jjtGetValue();
		}
		
		if (o1 instanceof Comparable<?>) {
			left = (Comparable<?>) o1;
		} else {
			throw new RuntimeException("Invalid expression");
		}
		
		if (o2 instanceof Comparable<?>) {
			right = (Comparable<?>) o2;
		} else {
			throw new RuntimeException("Invalid expression");
		}
		
		if (left instanceof UndefinedType || right instanceof UndefinedType) {
			node.jjtSetValue(UndefinedType.getInstance());
			return;
		}
		
		switch (op) {
		case UQLParserTreeConstants.JJTGT:
			node.jjtSetValue(left.compareTo(right) > 0);
			break;
		case UQLParserTreeConstants.JJTGE:
			node.jjtSetValue(left.compareTo(right) >= 0);
			break;
		case UQLParserTreeConstants.JJTLT:
			node.jjtSetValue(left.compareTo(right) < 0);
			break;
		case UQLParserTreeConstants.JJTLE:
			node.jjtSetValue(left.compareTo(right) <= 0);
			break;
		case UQLParserTreeConstants.JJTEQ:
			node.jjtSetValue(left.compareTo(right) == 0);
			break;
		case UQLParserTreeConstants.JJTNE:
			node.jjtSetValue(left.compareTo(right) != 0);
			break;
		}
	}
	
	public void processMultiplicativeExpression(SimpleNode node) {
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			switch(n.id) {
			case UQLParserTreeConstants.JJTUNARYEXPRESSION:
				processUnaryExpression(n);
				break;
			}
		}
		evaluateMultiplicativeExpression(node);
	}
	
	public void processUnaryExpression(SimpleNode node) {
		SimpleNode n = (SimpleNode) node.children[0];
		switch(n.id) {
		case UQLParserTreeConstants.JJTFN:
			processFunction(n);
			break;
		case UQLParserTreeConstants.JJTSTR:
			processStr(n);
			break;
		case UQLParserTreeConstants.JJTCOLUMNANDALIAS:
			processColumnAlias(n);
			break;
		case UQLParserTreeConstants.JJTNUMBER:
			processNumber(n);
			break;
		case UQLParserTreeConstants.JJTQ:
			break;
		case UQLParserTreeConstants.JJTGROUPEDCOMPAREEQUATION:
			processGroupedquation(n);
			break;
		}
		node.jjtSetValue(n.jjtGetValue());
	}
	
	private void processGroupedquation(SimpleNode node) {
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[0];
			switch(n.id) {
			case UQLParserTreeConstants.JJTCOMPAREEQUATION:
				processCompareEquation(n);
				node.jjtSetValue(n.jjtGetValue());
				break;
			}			
		}
	}
	
	private void evaluateMultiplicativeExpression(SimpleNode node) {
		int op = -1;
		Object currentOperand = 0L;
		
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			switch(n.id) {
			case UQLParserTreeConstants.JJTPLUS:
			case UQLParserTreeConstants.JJTMINUS:
			case UQLParserTreeConstants.JJTMUL:
			case UQLParserTreeConstants.JJTDIV:
				op = n.id;
				continue;
			default:
				break;
			}
			currentOperand = process(op, currentOperand, n.jjtGetValue());
		}		
		node.jjtSetValue(currentOperand);
	}
	
	Object process(int op, Object op1, Object op2) {
		if (op1 instanceof UndefinedType || op2 instanceof UndefinedType) {
			return UndefinedType.getInstance();
		}
		if (op >= 0 && op2 instanceof String) {
			throw new RuntimeException("Invalid expression");
		}
		
		if (op1 instanceof Long && op2 instanceof Double) {
			op1 = new Double((Long) op1);
		}
		
		if (op1 instanceof Double && op2 instanceof Long) {
			op2 = new Double((Long) op2);
		}
		
		if (op1 instanceof Long && op2 instanceof Integer) {
			op2 = new Long(((Integer) op2).longValue());
		}
		switch (op) {
			case UQLParserTreeConstants.JJTPLUS:
				if (op1 instanceof Double) {
					return (Double) op1 + (Double) op2;
				} else {
					return (Long) op1 + (Long) op2;
				}
			case UQLParserTreeConstants.JJTMINUS:
				if (op1 instanceof Double) {
					return (Double) op1 - (Double) op2;
				} else {
					return (Long) op1 - (Long) op2;
				}
			case UQLParserTreeConstants.JJTMUL:
				if (op1 instanceof Double) {
					return (Double) op1 * (Double) op2;
				} else {
					return (Long) op1 * (Long) op2;
				}
			case UQLParserTreeConstants.JJTDIV:
				if (op1 instanceof Double) {
					return (Double) op1 / (Double) op2;
				} else {
					return (Long) op1 / (Long) op2;
				}
		}
		
		return op2;
	}
	
	public static void processStr(SimpleNode node) {
		String s = node.jjtGetFirstToken().image;
		if (s == null || s.length() == 0) {
			return;
		}
		
		if (s.startsWith("\"") || s.startsWith("'")) {
			s = s.substring(1, s.length()-1);
		}
		node.jjtSetValue(s);
	}
	
	public void processColumnAlias(SimpleNode node) {
		String alias = "";
		Token t1 = node.jjtGetFirstToken();
		Token t2 = node.jjtGetLastToken();
		
		if (t1 != t2) {
			alias = t1.image;
		}
		CollectionAlias ca = aliasMap.get(alias);
		DBType dt = context.getValue(ca, t2.image, null);
		if (dt != null) {
			if (dt instanceof StringType) {
				node.jjtSetValue(((StringType) dt).get());
			} else if (dt instanceof IntType) {
				node.jjtSetValue(((IntType) dt).get());				
			} else if (dt instanceof LongType) {
				node.jjtSetValue(((LongType) dt).get());
			} else if (dt instanceof FloatType) {
				node.jjtSetValue(((FloatType) dt).get());
			} else if (dt instanceof DoubleType) {
				node.jjtSetValue(((DoubleType) dt).get());
			} else if (dt instanceof UndefinedType) {
				node.jjtSetValue(dt);
			} else {
				throw new RuntimeException("Invalid type");
			}
		} else {
			node.jjtSetValue(UndefinedType.getInstance());
		}
	}
	
	public static void processCA(SimpleNode node) {
		Token t1 = node.jjtGetFirstToken();
		Token t2 = node.jjtGetLastToken();
		if (t1 != t2) {
			node.jjtSetValue(t1.image);
		} else {
			node.jjtSetValue(t1.image + "." + t2.image);
		}
	}
	
	public static void processNumber(SimpleNode node) {
		Node[] children = node.children;
		if (children == null || children.length == 0) {
			throw new RuntimeException("Invalid syntax:");
		}
		boolean negative = false;
		String decimal = "";
		String precision = "";
			
		for (int i = 0; i < children.length; i++) {
			SimpleNode n = (SimpleNode) children[i];
			if (n.id == UQLParserTreeConstants.JJTMINUS) {
				negative = true;
			} else if (n.id == UQLParserTreeConstants.JJTDECIMAL) {
				decimal = n.jjtGetFirstToken().image;
			} else if (n.id == UQLParserTreeConstants.JJTPRECISION) {
				precision = n.jjtGetFirstToken().image;
			}
		}
		
		if ("".equals(precision)) {
			String value = negative ? "-"+decimal : decimal;
			node.jjtSetValue(Long.parseLong(value));
		} else {
			String value = negative ? "-"+decimal+"."+precision : decimal+"."+precision;
			node.jjtSetValue(Double.parseDouble(value));
		}
	}
	
	private void processFunction(SimpleNode node) {
		
		for (int i = 0; i < node.children.length; i++) {
			SimpleNode n = (SimpleNode) node.children[i];
			switch(n.id) {
			case UQLParserTreeConstants.JJTIDENTIFIER:
				processIdentifier(n);
				break;
			default:
				processMultiplicativeExpression(n);
				break;
			}
		}
		evaluateFunction(node);
	}
	
	public void evaluateFunction(SimpleNode node) {
		String functionName = (String) ((SimpleNode) node.children[0]).jjtGetValue();
		WonderDBFunction function = FunctionManager.getInstance().getFunction(functionName, node);
		function.process(context);
	}
	
	private void processIdentifier(SimpleNode node) {
		node.jjtSetValue(node.jjtGetFirstToken().image);
	}
	
	public Map<CollectionAlias, List<Integer>> getColumns(SimpleNode node, Map<String, CollectionAlias> fromMap, List<ResultSetColumn> resultSetColumns) {
		Map<CollectionAlias, List<Integer>> map = new HashMap<CollectionAlias, List<Integer>>();
		getColumns(node, fromMap, map, resultSetColumns, true);
		return map;
	}
	
	
	public void getColumns(SimpleNode node, Map<String, CollectionAlias> fromMap, 
			Map<CollectionAlias, List<Integer>> map, List<ResultSetColumn> resultSetColumns, boolean isResultSet) {
		StringBuilder sb = new StringBuilder();
		if (node.id == UQLParserTreeConstants.JJTTABLEDEF) {
			isResultSet = false;
		}

		if (node.id == UQLParserTreeConstants.JJTFN) {
			VirtualResultSetColumn rsc = new VirtualResultSetColumn();
			SimpleNode fnNode = (SimpleNode) node.children[0];
			rsc.node = fnNode;
			String fnName = (String) fnNode.jjtGetFirstToken().image;
			rsc.ca = new CollectionAlias("", "");
			WonderDBFunction fn = FunctionManager.getInstance().getFunction(fnName, node);
			rsc.function = fn;
			if (isResultSet) {
				resultSetColumns.add(rsc);
			}
		} else if (node.id == UQLParserTreeConstants.JJTCOLUMNANDALIAS) {
			Token f1 = node.jjtGetFirstToken();
			Token f2 = node.jjtGetLastToken();
			sb.append(f1.image);
			String alias = "";
			
			if (f1 != f2) {
				alias = f1.image;
			}
			
			CollectionAlias ca = fromMap.get(alias);
			List<Integer> list = map.get(ca);
			
			if (list == null) {
				list = new ArrayList<Integer>();
				map.put(ca, list);
			}
			
			CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
			
			ResultSetColumn rsc = null;
			int columnId = -1;
			if (!"*".equals(f2.image)) {
				columnId = colMeta.getColumnId(f2.image);
				
				if (!list.contains(columnId)) {
					list.add(columnId);
				}
				
				if (isResultSet) {
					rsc = new ResultSetColumn();
					rsc.ca = ca;
					rsc.columnId = columnId;
				}
			} else {
				synchronized (colMeta) {
					List<ColumnNameMeta> l1 = colMeta.getCollectionColumns();
					Iterator<ColumnNameMeta> iter = l1.iterator();
					while (iter.hasNext()) {
						ColumnNameMeta cc = iter.next();
						columnId = cc.getCoulmnId();

						if (!list.contains(columnId)) {
							list.add(columnId);
						}

						if (isResultSet) {
							rsc = new ResultSetColumn();
							rsc.ca = ca;
							rsc.columnId = columnId;
						}
					}
				}
			}
		}
		
		if (node.children != null) {
			for (int i = 0; i < node.children.length; i++) {
				SimpleNode n = (SimpleNode) node.children[i];
				getColumns(n, fromMap, map, resultSetColumns, isResultSet);
			}
		}
	}
	
	public AndExpression getAndExpression(SimpleNode startNode, Map<String, CollectionAlias> fromMap, List<ResultSetColumn> resultSetColumns) {
		SimpleNode selectNode = (SimpleNode) startNode.children[0];
		SimpleNode filterNode = null;
		List<BasicExpression> list = new ArrayList<BasicExpression>();
		for (int i = 0; i < selectNode.children.length; i++) {
			SimpleNode n = (SimpleNode) selectNode.children[i];
			if (n.id == UQLParserTreeConstants.JJTFILTEREXPRESSION) {
				filterNode = n;
				break;
			}
		}
		
		if (filterNode == null) {
			return new AndExpression(new ArrayList<BasicExpression>());
		}
		
		SimpleNode compareEq = (SimpleNode) filterNode.children[0];
		for(int i = 0; i < compareEq.children.length; i++) {
			BasicExpression exp = buildExpression((SimpleNode) compareEq.children[i], fromMap, resultSetColumns);
			list.add(exp);
		}
		
		return new AndExpression(list);
	}
	
	private BasicExpression buildExpression(SimpleNode eqEq, Map<String, CollectionAlias> fromMap, List<ResultSetColumn> resultSetColumns) {
		SimpleNode left = null;
		SimpleNode right = null;
		int op = -1;
		for (int i = 0; i < eqEq.children.length; i++) {
			SimpleNode node = (SimpleNode) eqEq.children[i];
			if (op == -1 && node.id == UQLParserTreeConstants.JJTCOMPAREEQUATION) {
				left = node;
			} else if (op >= 0 && node.id == UQLParserTreeConstants.JJTCOMPAREEQUATION) {
				right = node;
			} else  {
				op = node.id;
			}
		}

		Operand l = buildOperand(left, fromMap, resultSetColumns);
		Operand r = buildOperand(right, fromMap, resultSetColumns);
		return new BasicExpression(l, r, op);
		
	}
	
	private Operand buildOperand(SimpleNode node, Map<String, CollectionAlias> fromMap, List<ResultSetColumn> resultSetColumns) {
		Map<CollectionAlias, List<Integer>> columns = getColumns(node, fromMap, resultSetColumns);
		int count = getColumnCount(columns);
		if (count == 1) {
			Iterator<CollectionAlias> iter = columns.keySet().iterator();
			List<Integer> l = null;
			CollectionAlias ca = null;
			while (iter.hasNext()) {
				ca = iter.next();
				l = columns.get(ca);
			}
			return new VariableOperand(ca, l.get(0), null);
		} else if (count == 0) {
			processMultiplicativeExpression(node);
			return new StaticOperand(new StringType((String) node.jjtGetValue()));
		} else {
			throw new RuntimeException("Invalid syntax");
		}
	}

	private int getColumnCount(Map<CollectionAlias, List<Integer>> columns) {
		Iterator<List<Integer>> iter = columns.values().iterator();
		int count = 0;
		while (iter.hasNext()) {
			List<Integer> list = iter.next();
			count = count + list.size();
		}
		
		return count;
	}
	
	public List<SimpleNode> getSelectStmts(SimpleNode start) {
		List<SimpleNode> list = new ArrayList<SimpleNode>();
		if (start.id == UQLParserTreeConstants.JJTSELECTSTMT) {
			list.add(start);
			return list;
		}
		for (int i = 0; i < start.children.length; i++) {
			SimpleNode n = (SimpleNode) start.children[i];
			if (n.id == UQLParserTreeConstants.JJTSELECTSTMT) {
				list.add(n);
			}
		}
		return list;
	}
	
	public SimpleNode getStmt(SimpleNode node) {
		if (node.id != UQLParserTreeConstants.JJTSTART) {
			return node;
		}
		return (SimpleNode) node.children[0];
	}
	
	public static void dump(ExpressionNode node) {
		dump(node, "");
	}
	
	public static void dump(ExpressionNode node, String indent) {
		System.out.println(indent);
		
		if (node == null) {
			return;
		}
		
		if (node.currentNode.id == UQLParserTreeConstants.JJTEQUALITYEQUATION) {
			for (int i = 0; i < node.currentNode.children.length; i++) {
				SimpleNode n = (SimpleNode) node.currentNode.children[i];
				switch (n.id) {
					case UQLParserTreeConstants.JJTCOLUMNANDALIAS:
						QueryEvaluator.processCA(n);
						break;
					case UQLParserTreeConstants.JJTEQ:
						n.jjtSetValue("=");
						break;
					case UQLParserTreeConstants.JJTPLUS:
						n.jjtSetValue("+");
						break;
					case UQLParserTreeConstants.JJTNUMBER:
						QueryEvaluator.processNumber(n);
						break;
					default:
						break;
				}
				System.out.print(n.jjtGetValue() + " ");
			}
			System.out.println("");
		} else {
			for (int i = 0; i < node.andList.size(); i++) {
				dump(node.andList.get(i), indent+" ");
			}
			for (int i = 0; i < node.orList.size(); i++) {
				dump(node.orList.get(i), indent+" ");
			}
		}
	}
}
