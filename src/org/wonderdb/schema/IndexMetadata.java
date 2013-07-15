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
package org.wonderdb.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.record.manager.RecordId;
import org.wonderdb.cluster.Shard;
import org.wonderdb.collection.BTree;
import org.wonderdb.file.FileBlockManager;


public class IndexMetadata extends SchemaObjectImpl {
//	BTree tree = null;
	Index index;
	RecordId recordId;
	ConcurrentMap<Shard, BTree> shardBtreeMap = new ConcurrentHashMap<Shard, BTree>();
	
	public IndexMetadata(Index index, int id) {
		this(index, FileBlockManager.getInstance().getDefaultFileName(), id);
	}

	public IndexMetadata(Index index, String fileName, int id) {
		super(index.getIndexName(), "IS", id);
		this.index = index;
	}

	public IndexMetadata(Index index, String fileName, int id, BlockPtr contBlockPtr, BlockPtr headPtr, BlockPtr rootPtr) {
		super(index.getIndexName(), "IS", id);
		this.index = index;
	}

	public IndexMetadata(Index index, String serializerName, int id, BlockPtr headPtr, BlockPtr rootPtr) {
		super(index.getIndexName(), serializerName, id);
		this.index = index;
	}

	public Index getIndex() {
		return index;
	}
	
	public BTree getIndexTree(Shard shard) {
		return shardBtreeMap.get(shard);
	}
	
	public String getSerializerName() {
		return "ims";
	}	
	
	public void addShard(Shard shard, BTree tree) {
		shardBtreeMap.put(shard, tree);
	}
	
	public byte getFileId(Shard shard) {
		return FileBlockManager.getInstance().getId(shard);	
	}
	
	public int getBlockSize() {
		List<BTree> list = new ArrayList<BTree>(shardBtreeMap.values());
		Shard shard = list.get(0).getShard();
		return FileBlockManager.getInstance().getEntry(shard).getBlockSize();
	}
}	
