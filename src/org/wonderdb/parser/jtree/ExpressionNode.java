package org.wonderdb.parser.jtree;

import java.util.List;

import org.wonderdb.expression.BasicExpression;

public class ExpressionNode {
	SimpleNode currentNode;
	List<ExpressionNode> andList;
	List<ExpressionNode> orList;
	BasicExpression expression = null;
	
	public ExpressionNode(SimpleNode currentNode, List<ExpressionNode> andList, List<ExpressionNode> orList) {
		this.currentNode = currentNode;
		this.andList = andList;
		this.orList = orList;
		populateNodes();
	}
	
	private void populateNodes() {
		if (andList != null && andList.size() > 0 && orList != null && orList.size() > 0) {
			return;
		}
		
		if (currentNode != null && currentNode.children != null && currentNode.children.length == 3) {
			SimpleNode left = (SimpleNode) currentNode.children[0];
			SimpleNode right = (SimpleNode) currentNode.children[2];
			SimpleNode op = (SimpleNode) currentNode.children[1];
			
//			TreeOperand l = new TreeOperand(left);
//			TreeOperand r = new TreeOperand(right);
//			expression = new BasicExpression(l, r, op.id);
		}
	}
	
	public boolean isComposite() {
		if (andList.size() > 0 || orList.size() > 0) {
			return true;
		}
		
		for (int i = 0; i < andList.size(); i++) {
			ExpressionNode node = andList.get(i);
			if (node.isComposite()) {
				return true;
			}
		}
		
		for (int i = 0; i < orList.size(); i++) {
			ExpressionNode node = orList.get(i);
			if (node.isComposite()) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean isMultilevel() {
		for (int i = 0; i < andList.size(); i++) {
			ExpressionNode node = andList.get(i);
			if (node.andList.size() > 0 || node.orList.size() > 0) {
				return true;
			}
		}

		for (int i = 0; i < orList.size(); i++) {
			ExpressionNode node = orList.get(i);
			if (node.andList.size() > 0 || node.orList.size() > 0) {
				return true;
			}
		}
		
		return false;
	}	
}
