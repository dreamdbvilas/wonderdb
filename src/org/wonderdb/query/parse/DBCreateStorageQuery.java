package org.wonderdb.query.parse;

import org.wonderdb.cluster.ClusterManagerFactory;
import org.wonderdb.expression.AndExpression;
import org.wonderdb.metadata.StorageMetadata;
import org.wonderdb.parser.jtree.Node;
import org.wonderdb.parser.jtree.SimpleNode;
import org.wonderdb.parser.jtree.SimpleNodeHelper;
import org.wonderdb.parser.jtree.UQLParserTreeConstants;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.types.FileBlockEntry;

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



public class DBCreateStorageQuery extends BaseDBQuery {
	String storageName = null;
	int blockSize = -1;
	boolean isDefault = false;
	
	public DBCreateStorageQuery(String q, Node n) {
		super(q, (SimpleNode) n, -1, null);
		SimpleNode query = (SimpleNode) n;
		SimpleNode node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTSTORAGENAME);
		if (node == null) {
			throw new RuntimeException("Invalid syntax");
		}
		storageName = node.jjtGetFirstToken().image;
		storageName = storageName.substring(1, storageName.length()-1);
		node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTSTORAGESIZE);
		if (node != null) {
			blockSize = Integer.parseInt(node.jjtGetFirstToken().image);
		} else {
			blockSize = WonderDBPropertyManager.getInstance().getDefaultBlockSize();
		}
		node = SimpleNodeHelper.getInstance().getFirstNode(query, UQLParserTreeConstants.JJTDEFAULTSTORAGE);
		if (node != null) {
			isDefault = true;
		}
	}
	
	public void execute() {
		FileBlockEntry entry = new FileBlockEntry();
		entry.setBlockSize(blockSize);
		entry.setDefaultFile(isDefault);
		entry.setFileName(storageName);
		StorageMetadata.getInstance().add(entry);
		ClusterManagerFactory.getInstance().getClusterManager().createStorage(storageName, blockSize, isDefault);
	}
	
	public String getFileName() {
		return storageName;
	}

	@Override
	public AndExpression getExpression() {
		// TODO Auto-generated method stub
		return null;
	}
}
