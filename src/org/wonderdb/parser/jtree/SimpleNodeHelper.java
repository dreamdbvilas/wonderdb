package org.wonderdb.parser.jtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.wonderdb.expression.BasicExpression;
import org.wonderdb.expression.Expression;
import org.wonderdb.expression.Operand;
import org.wonderdb.expression.TreeOperand;
import org.wonderdb.expression.VariableOperand;
import org.wonderdb.parser.TableDef;
import org.wonderdb.parser.UpdateSetExpression;
import org.wonderdb.query.parse.CollectionAlias;
import org.wonderdb.query.parse.StaticOperand;
import org.wonderdb.schema.CollectionMetadata;
import org.wonderdb.schema.SchemaMetadata;
import org.wonderdb.types.StringType;

public class SimpleNodeHelper {
	private static SimpleNodeHelper instance = new SimpleNodeHelper();
	
	private SimpleNodeHelper() {
	}
	
	public static SimpleNodeHelper getInstance() {
		return instance;
	}

	public SimpleNode getFirstNode(SimpleNode node, int nodeId) {
		if (node == null) {
			return null;
		}
		
		if (node.id == nodeId) {
			return node;
		}
		
		Node[] children = node.children;
		if (children == null) {
			return null;
		}
		
		for (int i = 0; i < children.length; i++) {
			SimpleNode n = (SimpleNode) children[i];
			if (n.id == nodeId) {
				return n;
			}
			n = getFirstNode(n, nodeId);
			if (n != null && n.id == nodeId) {
				return n;
			}
		}
		
		return null;
	}
	
	public Map<String, CollectionAlias> getFromMap(SimpleNode root) {
		Map<String, CollectionAlias> map = new HashMap<>();
		List<SimpleNode> list = new ArrayList<>();
		traverse(root, UQLParserTreeConstants.JJTTABLEDEF, list);
		
		for (int i = 0; i < list.size(); i++) {
			TableDef tableDef = getTableDef(list.get(i));
			CollectionAlias ca = new CollectionAlias(tableDef.table, tableDef.alias);
			map.put(tableDef.alias, ca);
		}
		return map;
	}
	
	public void traverse(SimpleNode root, int nodeId, List<SimpleNode> nodeList) {
		if (root == null) {
			return;
		}
		
		if (root.id == nodeId) {
			nodeList.add(root);
		}
		
		Node[] children = root.children;
		if (children != null) {
			for (int i = 0; i < children.length; i++) {
				traverse((SimpleNode) children[i], nodeId, nodeList);
			}
		}
		
	}
	
	public TableDef getTableDef(SimpleNode node) {
		Token f = node.jjtGetFirstToken();
		Token l = node.jjtGetLastToken();
		TableDef tableDef = new TableDef();
		
		tableDef.table = f.image;
		tableDef.alias = "";
		if (f != l) {
			tableDef.alias = l.image;
		} 
		return tableDef;
	}
	
	public void getNodes(SimpleNode root, int nodeId, List<SimpleNode> nodeList) {
		if (root == null) {
			return;
		}
		
		if (root.id == nodeId) {
			nodeList.add(root);
		}
		
		Node[] children = root.children;
		if (children != null) {
			for (int i = 0; i < children.length; i++) {
				SimpleNode node = (SimpleNode) children[i];
				getNodes(node, nodeId, nodeList);
			}
		}
	}
	
	public List<TableDef> getTables(SimpleNode selectNode) {
		List<TableDef> retList = new ArrayList<>();
		List<SimpleNode> tableDefNodes = new ArrayList<>();
		getNodes(selectNode, UQLParserTreeConstants.JJTTABLEDEF, tableDefNodes);
		
		for (int i = 0; i < tableDefNodes.size(); i++) {
			SimpleNode node = tableDefNodes.get(i);
				retList.add(getTableDef(node));
			}			
		
		return retList;
	}
	

	
	public SimpleNode copyTree(SimpleNode node) {
		SimpleNode n = new SimpleNode(node.id);
		n.jjtSetFirstToken(node.jjtGetFirstToken());
		n.jjtSetLastToken(node.jjtGetLastToken());
		n.jjtSetParent(node.jjtGetParent());
		n.jjtSetValue(node.jjtGetValue());
		
		if (node.children != null) {
			for (int i = 0; i < node.children.length; i++) {
				SimpleNode n1 = copyTree((SimpleNode) node.children[i]);
				n.jjtAddChild(n1, i);
			}
		}
		return n;
	}

	public void makeGrandParentAsParent(SimpleNode node) {
		if (node == null) {
			return;
		}
		
		SimpleNode parent = (SimpleNode) node.jjtGetParent();
		if (parent == null) {
			return;
		}
		
		parent = (SimpleNode) parent.jjtGetParent();
		if (parent == null) {
			return;
		}
		
		parent.jjtAddChild(node, parent.children.length);
	}
	
//	public ExpressionNode shouldQueryRewriteStartNode(SimpleNode start) {
//		SimpleNode selectNode = (SimpleNode) start.children[0];
//		SimpleNode compareNode = null;
//		for (int i = 0; i < selectNode.children.length; i++) {
//			compareNode = (SimpleNode) selectNode.children[i];
//			if (compareNode.id == UQLParserTreeConstants.JJTFILTEREXPRESSION) {
//				break;
//			}			
//		}
//		
//		if (compareNode.id != UQLParserTreeConstants.JJTFILTEREXPRESSION) {
//			return null;
//		}
//		if (compareNode.children != null && compareNode.children.length > 0) {
//			return convertToExpressionNode((SimpleNode) compareNode.children[0]);
//		}
//		return new ExpressionNode(start, new ArrayList<>(), new ArrayList<>());
//	}
	
	public void orderAndOr(SimpleNode start) {
		if (andOrOutofOrder(start)) {
			order(start);
		} 
		
		if (start.children != null) {
			for (int i = 0; i < start.children.length; i++) {
				orderAndOr((SimpleNode) start.children[i]);
			}
		}
	}
	
	public void order(SimpleNode start) {
		List<List<SimpleNode>> orNodes = new ArrayList<List<SimpleNode>>();
		List<List<SimpleNode>> andNodes = new ArrayList<List<SimpleNode>>();
		List<SimpleNode> temp = new ArrayList<SimpleNode>();
		
		for (int i = 0; i < start.children.length; i++) {
			SimpleNode n = (SimpleNode) start.children[i];
			if (n.id != UQLParserTreeConstants.JJTAND || n.id != UQLParserTreeConstants.JJTOR) {
				temp.add(n);
			} else if (n.id == UQLParserTreeConstants.JJTAND) {
				andNodes.add(temp);
				temp = new ArrayList<SimpleNode>();
			} else if (n.id == UQLParserTreeConstants.JJTOR) {
				orNodes.add(temp);
				temp = new ArrayList<SimpleNode>();				
			}
		}
		
		List<SimpleNode> finalList = new ArrayList<SimpleNode>();
		for (int i = 0; i < andNodes.size(); i++) {
			temp = andNodes.get(i);
			finalList.addAll(temp);
			if (i < andNodes.size()-1) {
				finalList.add(new SimpleNode(UQLParserTreeConstants.JJTAND));
			}
		}

		if (finalList.size() > 0) {
			finalList.add(new SimpleNode(UQLParserTreeConstants.JJTOR));
		}
		
		for (int i = 0; i < orNodes.size(); i++) {
			temp = orNodes.get(i);
			finalList.addAll(temp);
			if (i < orNodes.size()-1) {
				finalList.add(new SimpleNode(UQLParserTreeConstants.JJTOR));
			}
		}		
	}
	
	public boolean andOrOutofOrder(SimpleNode node) {
		boolean foundOr = false;
		if (node.children != null) {
			for (int i = 0; i < node.children.length; i++) {
				SimpleNode n = (SimpleNode) node.children[i];
				if (n.id != UQLParserTreeConstants.JJTAND && n.id != UQLParserTreeConstants.JJTOR) {
					continue;
				}
				if (n.id == UQLParserTreeConstants.JJTAND && foundOr) {
					return true;
				} else {
					foundOr = true;
				}
			}
		}
		return false;
	}

//	public ExpressionNode convertToExpressionNode(SimpleNode start) {
//		List<ExpressionNode> andList = new ArrayList<>();
//		List<ExpressionNode> orList = new ArrayList<>();
//		
//		ExpressionNode currentNode = null;
//		int currentOp = -1;
//		if (start.children != null) {
//			for (int i = 0; i < start.children.length; i++) {			
//				SimpleNode child = (SimpleNode) start.children[i];
//				if (child.id == UQLParserTreeConstants.JJTAND && currentNode != null)	{
//					andList.add(currentNode);
//					currentOp = child.id;
//				} else if (child.id == UQLParserTreeConstants.JJTOR && currentNode != null) {
//					orList.add(currentNode);
//					currentOp = child.id;
//				}
//				currentNode = convertToExpressionNode((SimpleNode) start.children[i]);
//			}
//		}		
//		if (currentOp == UQLParserTreeConstants.JJTAND) {
//			andList.add(currentNode);
//		} else if (currentOp == UQLParserTreeConstants.JJTOR) {
//			orList.add(currentNode);
//		} 
//		return new ExpressionNode(start, andList, orList);
//	}
	
//	public ExpressionNode shouldQueryRewrite(SimpleNode start) {
//		Set<SimpleNode> andList = new HashSet<SimpleNode>();
//		Set<SimpleNode> orList = new HashSet<SimpleNode>();
//
//		List<ExpressionNode> aList = new ArrayList<ExpressionNode>(andList.size());
//		List<ExpressionNode> oList = new ArrayList<ExpressionNode>(orList.size());
//
//		SimpleNode node = start;
//		if (node.children == null) {
//			return new ExpressionNode(node, aList, oList);
//		}
//
//		int posn = 1;
//		node = (SimpleNode) node.children[0];
//		if (node.id != UQLParserTreeConstants.JJTCOMPAREEQUATION) {
//			return new ExpressionNode((SimpleNode) node.parent, aList, oList);
//		}
//		
//		int currentId = UQLParserTreeConstants.JJTAND;
//		
//		while (true) {
//			if (node.children.length == 1) {
//				andList.add((SimpleNode) node.children[0]);
//				break;
//			}
//
//			if (posn < node.children.length) {
//				currentId = ((SimpleNode) node.children[posn]).id;
//			}
//			
//			SimpleNode current = (SimpleNode) node.children[posn-1];
//			if (currentId == UQLParserTreeConstants.JJTAND) {
//				andList.add(current);
//			} else {
//				orList.add(current);
//			}
//			if (posn >= node.children.length) {
//				break;
//			}
//			posn = posn + 2;
//		}
//			
//		Iterator<SimpleNode> iter = andList.iterator();
//		while (iter.hasNext()) {
//			ExpressionNode n = shouldQueryRewrite(iter.next());
//			aList.add(n);
//		}
//
//		iter = orList.iterator();
//		while (iter.hasNext()) {
//			ExpressionNode n = shouldQueryRewrite(iter.next());
//			oList.add(n);
//		}
//		
//		return new ExpressionNode(node, aList, oList);
//	}
	
//	public void flattenNode(ExpressionNode node) {
//		while (node != null && (flatenParentAndChildSameType(node) || flattenParentAndChildOr(node)));
//	}
	
//	public boolean flattenParentAndChildOr(ExpressionNode node) {
//
//		boolean retVal = false;
//
//		for (int i = 0; i < node.andList.size(); i++) {
//			ExpressionNode en = node.andList.get(i);
//			retVal = retVal || flattenParentAndChildOr(en);
//		}
//		
//		for (int i = 0; i < node.orList.size(); i++) {
//			ExpressionNode en = node.orList.get(i);
//			retVal = retVal || flattenParentAndChildOr(en);
//		}
//		List<ExpressionNode> newOrList = new ArrayList<ExpressionNode>(node.orList);
//
//		for (int i = 0; i < node.andList.size(); i++) {
//			ExpressionNode en = node.andList.get(i);
//			if (isOrOr(en)) {
//				List<ExpressionNode> al = null;
//				ExpressionNode n = null;
//				
//				for (int j = 0; j < en.orList.size(); j++) {
//					al = new ArrayList<ExpressionNode>(node.andList);
//					al.add(en.orList.get(j));
//					n = new ExpressionNode(node.currentNode, al, new ArrayList<ExpressionNode>());
//					newOrList.add(n);
//				}
//				retVal = true;
//			}
//		}
//		
//		if (retVal) {
//			node.andList = new ArrayList<ExpressionNode>();
//			node.orList = newOrList;
//		}
//		return retVal;
//	}
	
	
//	public boolean flatenParentAndChildSameType(ExpressionNode node) {
////		if (true) {
////			return false;
////		}
//		// case I and-and/ or - or -> Just move children to parent.
//		for (int i = 0; i < node.andList.size(); i++) {
//			ExpressionNode en = node.andList.get(i);
//			flatenParentAndChildSameType(en);
//		}
//		
//		for (int i = 0; i < node.orList.size(); i++) {
//			ExpressionNode en = node.orList.get(i);
//			flatenParentAndChildSameType(en);
//		}
//		
//		List<ExpressionNode> removeAndList = new ArrayList<ExpressionNode>();
//		List<ExpressionNode> removeOrList = new ArrayList<ExpressionNode>();
//		
//		for (int i = 0; i < node.andList.size(); i++) {
//			ExpressionNode en = node.andList.get(i);
//			if (isAndAnd(en) && en.andList.size() > 0) {
//				node.andList.addAll(en.andList);
//				removeAndList.add(en);
//			}
//		}
//		
//		for (int i = 0; i < node.orList.size(); i++) {
//			ExpressionNode en = node.orList.get(i);
//			if (isOrOr(en) && node.orList.size() > 0) {
//				node.orList.addAll(en.orList);
//				node.orList.addAll(en.andList);
//				removeOrList.add(en);			
//			}
//		}
//		
//		node.andList.removeAll(removeAndList);
//		node.orList.removeAll(removeOrList);
//
//		return removeAndList.size() > 0 || removeOrList.size() > 0;
//	}
	
//	private boolean isAndAnd(ExpressionNode node) {
//		if (node.orList.size() != 0) {
//			return false;
//		}
//		
//		return true;
//	}
//	
//	private boolean isOrOr(ExpressionNode node) {
//		if (node.orList.size() > 0 && node.andList.size() == 0) {
//			return true;
//		} else if (node.andList.size() == 1) {
//			return true;
//		}
//		
//		return false;
//	}	
	
	public List<SimpleNode> breakAnds(SimpleNode compareNode) {
		List<SimpleNode> retList = new ArrayList<>();
		if (compareNode == null) {
			return retList;
		}
		
		Node[] children = compareNode.children;
		for (int i = 0; i < children.length; i++) {
			SimpleNode node = (SimpleNode) children[i];
			
			if (node.id == UQLParserTreeConstants.JJTOR) {
				return new ArrayList<>();
			}
			
			if (node.id == UQLParserTreeConstants.JJTEQUALITYEQUATION) {
				retList.add(node);
			}
		}
		
		return retList;
	}
	
	public List<BasicExpression> buildAndExpressionList(SimpleNode filterNode, List<CollectionAlias> caList) {
		List<SimpleNode> list = breakAnds(filterNode);
		List<BasicExpression> retList = new ArrayList<>();
		
		for (int i = 0; i < list.size(); i++) {
			SimpleNode node = list.get(i);
			Node[] children = node.children;
			if (children == null || children.length != 3) {
				continue;
			}
			SimpleNode left = (SimpleNode) children[0];
			SimpleNode op = (SimpleNode) children[1];
			SimpleNode right = (SimpleNode) children[2];
			if (left == null || op == null || right == null) {
				continue;
			}
			
			if (op.id != UQLParserTreeConstants.JJTEQ &&
				op.id != UQLParserTreeConstants.JJTLE &&
				op.id != UQLParserTreeConstants.JJTLT &&
				op.id != UQLParserTreeConstants.JJTGE &&
				op.id != UQLParserTreeConstants.JJTGT) {
				continue;
			}
			
			Operand leftOperand = getOperand(left, caList);
			Operand rightOperand = getOperand(right, caList);
			int operator = convertToExpOp(op.id);
			BasicExpression exp = new BasicExpression(leftOperand, rightOperand, operator);
			retList.add(exp);
		}
		return retList;
	}
	
	private Operand getOperand(SimpleNode node, List<CollectionAlias> caList) {
		Operand operand = null;
		if (getColumnAliasNode(node) != null) {
			operand = getColumnId(node, caList);
		} else if (isStaticNode(node)) {
			QueryEvaluator qe = new QueryEvaluator(null, null);
			qe.processMultiplicativeExpression(node);
			Object val = node.jjtGetValue();
			operand = new StaticOperand(new StringType(val != null ? val.toString() : null));
		} else {
			operand = new TreeOperand();
		}
		return operand;
	}
	
	private SimpleNode getColumnAliasNode(SimpleNode node) {
			Node[] children = node.children;
			
			if (children == null || node.children == null || node.children.length == 0) {
				if (node.id == UQLParserTreeConstants.JJTCOLUMNANDALIAS) {
					return node;
				} else {
					return null;
				}
			} 
			
			if (node.children.length > 1) {
				return null;
			}
			
			return getColumnAliasNode((SimpleNode) node.children[0]); 
	}
	
	int convertToExpOp(int nodeOp) {
		switch (nodeOp) {
			case UQLParserTreeConstants.JJTEQ:
				return Expression.EQ;
		
			case UQLParserTreeConstants.JJTLE:
				return Expression.LE;
				
			case UQLParserTreeConstants.JJTGE:
				return Expression.GE;
				
			case UQLParserTreeConstants.JJTGT:
				return Expression.GT;
				
			case UQLParserTreeConstants.JJTLT:
				return Expression.LT;
				
//			case UQLParserTreeConstants.LIKE:
//				return Expression..LIKE;
		}
		
		return -1;
	}
	
	
	public boolean isStaticNode(SimpleNode node) {
		List<SimpleNode> nodeList = new ArrayList<>();
		getNodes(node, UQLParserTreeConstants.JJTCOLUMNANDALIAS, nodeList);
		return nodeList.size() == 0 ? true : false;
	}
	
	public VariableOperand getColumnId(SimpleNode columnAliasNode, List<CollectionAlias> caList) {
		String alias = columnAliasNode.firstToken.image;
		String columnName = columnAliasNode.lastToken.image;
		
		if (alias == columnName) {
			alias = "";
		}
		
		CollectionAlias ca = getCollectionAlias(alias, caList);
		CollectionMetadata colMeta = SchemaMetadata.getInstance().getCollectionMetadata(ca.getCollectionName());
		int colId = colMeta.getColumnId(columnName);
		return new VariableOperand(ca, colId, null);
	}
	
	public CollectionAlias getCollectionAlias(String alias, List<CollectionAlias> caList) {
		for (int i = 0; i < caList.size(); i++) {
			CollectionAlias ca = caList.get(i);
			if (ca.getAlias().equals(alias)) {
				return ca;
			}
		}
		
		return null;
	}

	public List<UpdateSetExpression> getUpdateSet(List<SimpleNode> updateSetNodes, List<CollectionAlias> caList) {
		List<UpdateSetExpression> retList = new ArrayList<>();
		
		for (int i = 0; i < updateSetNodes.size(); i++) {
			SimpleNode node = updateSetNodes.get(i);
			if (node.children == null || node.children.length != 2) {
				continue;
			}
			
			UpdateSetExpression exp = new UpdateSetExpression();
			SimpleNode leftChild = (SimpleNode) node.children[0];
			if (leftChild.id == UQLParserTreeConstants.JJTCOLUMNANDALIAS) {
				VariableOperand vo = getColumnId(leftChild, caList);
				exp.column = vo;
			} else {
				throw new RuntimeException("Invalid update set value");
			}
			
			SimpleNode rightChild = (SimpleNode) node.children[1];
			exp.value = rightChild;
			retList.add(exp);
		}
		
		return retList;
	}
	
	public int getConstCount(SimpleNode node, int constId) {
		int count = 0;
		if (node == null) {
			return 0;
		}
		
		if (node.firstToken.kind == constId) {
			count = 1;
		}
		
		Node[] children = node.children;
		if (children == null) {
			return count;
		}
		
		for (int i = 0; i < children.length; i++) {
			SimpleNode n = (SimpleNode) children[i];
			count = count + getConstCount(n, constId);
		}
		
		return count;
	}
}
