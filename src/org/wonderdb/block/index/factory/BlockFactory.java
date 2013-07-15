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
package org.wonderdb.block.index.factory;

import java.util.HashSet;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.block.BlockPtr;
import org.wonderdb.block.CacheableList;
import org.wonderdb.block.impl.base.SingleBlockPtr;
import org.wonderdb.block.impl.disk.DiskRecordBlock;
import org.wonderdb.block.index.IndexBranchBlock;
import org.wonderdb.block.index.IndexLeafBlock;
import org.wonderdb.block.index.impl.disk.DiskIndexBranchBlock;
import org.wonderdb.block.index.impl.disk.DiskIndexLeafBlock;
import org.wonderdb.block.record.RecordBlock;
import org.wonderdb.cache.CacheEntryPinner;
import org.wonderdb.cache.CacheHandler;
import org.wonderdb.cache.PrimaryCacheHandlerFactory;
import org.wonderdb.cache.PrimaryCacheResourceProvider;
import org.wonderdb.cache.PrimaryCacheResourceProviderFactory;
import org.wonderdb.cache.SecondaryCacheHandlerFactory;
import org.wonderdb.cache.SecondaryCacheResourceProvider;
import org.wonderdb.cache.SecondaryCacheResourceProviderFactory;
import org.wonderdb.cluster.Shard;
import org.wonderdb.file.FileBlockManager;
import org.wonderdb.schema.CollectionTailMgr;
import org.wonderdb.seralizers.IndexBlockSerializer;
import org.wonderdb.seralizers.block.SerializedBlock;
import org.wonderdb.seralizers.block.SerializedIndexBranchContinuationBlock;
import org.wonderdb.seralizers.block.SerializedIndexLeafContinuationBlock;
import org.wonderdb.seralizers.block.SerializedRecordLeafBlock;
import org.wonderdb.server.WonderDBServer;


public class BlockFactory {
	private static BlockFactory instance = new BlockFactory();
	CacheHandler<ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();
	CacheHandler<CacheableList> primaryCacheHandler = PrimaryCacheHandlerFactory.getInstance().getCacheHandler();
	SecondaryCacheResourceProvider secondaryResourceProvider = SecondaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	PrimaryCacheResourceProvider primaryResourceProvider = PrimaryCacheResourceProviderFactory.getInstance().getResourceProvider();
	public static BlockFactory getInstance() {
		return instance;
	}
	
	public IndexLeafBlock createIndexLeafBlock(int indxSchemaId, Shard shard, Set<BlockPtr> pinnedBlocks) {
		IndexLeafBlock b = null;
		try {
//			IndexMetadata meta = SchemaMetadata.getInstance().getIndex(indxSchemaId);
			byte fileId = FileBlockManager.getInstance().getId(shard);
			long posn = FileBlockManager.getInstance().getNextBlock(shard);
			BlockPtr ptr = new SingleBlockPtr(fileId, posn);
			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
			b = new DiskIndexLeafBlock(indxSchemaId, ptr);
			b.setNext(null);
			primaryCacheHandler.addIfAbsent(b);
			SerializedBlock ref = secondaryResourceProvider.getResource(ptr, SerializedBlock.INDEX_LEAF_BLOCK, System.currentTimeMillis());
			primaryResourceProvider.getResource(ptr, 1);
			SerializedIndexLeafContinuationBlock sicb = new SerializedIndexLeafContinuationBlock(ref, pinnedBlocks, true, false);
			sicb.setNextPtr(null);
			IndexBlockSerializer.getInstance().toBytes(b, sicb.getDataBuffer(), indxSchemaId);
			secondaryCacheHandler.addIfAbsent(ref);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return b;
		
	}
	
	public IndexBranchBlock createIndexBranchBlock(int idxSchemaId, Shard shard, Set<BlockPtr> pinnedBlocks) {
		IndexBranchBlock b = null;
		try {
//			IndexMetadata idx = SchemaMetadata.getInstance().getIndex(idxSchemaId);
			
			byte fileId = FileBlockManager.getInstance().getId(shard);
			long posn = FileBlockManager.getInstance().getNextBlock(shard);
			BlockPtr ptr = new SingleBlockPtr(fileId, posn);
			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
			b = new DiskIndexBranchBlock(idxSchemaId, ptr);
			primaryCacheHandler.addIfAbsent(b);
			SerializedBlock ref = secondaryResourceProvider.getResource(ptr, SerializedBlock.BRANCH_BLOCK, System.currentTimeMillis());
			primaryResourceProvider.getResource(ptr, 1);
			SerializedIndexBranchContinuationBlock sibc = new SerializedIndexBranchContinuationBlock(ref, pinnedBlocks, true, false);
			IndexBlockSerializer.getInstance().toBytes(b, sibc.getDataBuffer(), idxSchemaId);
			secondaryCacheHandler.addIfAbsent(ref);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return b;
	}
		
	public IndexBranchBlock createIndexBranchBlock(BlockPtr p, Set<BlockPtr> pinnedBlocks) {
		IndexBranchBlock b = null;
		try {
			BlockPtr ptr = p;
			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
			b = new DiskIndexBranchBlock(-1, ptr);
			primaryCacheHandler.addIfAbsent(b);
			SerializedBlock ref = secondaryResourceProvider.getResource(ptr, SerializedBlock.BRANCH_BLOCK, System.currentTimeMillis());
			primaryResourceProvider.getResource(ptr, 1);
			SerializedIndexBranchContinuationBlock sibc = new SerializedIndexBranchContinuationBlock(ref, pinnedBlocks, true, false);
			IndexBlockSerializer.getInstance().toBytes(b, sibc.getDataBuffer(), -1);
			secondaryCacheHandler.addIfAbsent(ref);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return b;
	}
		
	public RecordBlock createRecordBlock(int schemaId, Shard shard, Set<BlockPtr> pinnedBlocks) {
		RecordBlock b = null;
		try {
//			CollectionMetadata meta = SchemaMetadata.getInstance().getCollectionMetadata(schemaId);
			byte fileId = FileBlockManager.getInstance().getId(shard);
			long posn = FileBlockManager.getInstance().getNextBlock(shard);
			BlockPtr ptr = new SingleBlockPtr(fileId, posn);
			CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
			b = new DiskRecordBlock(schemaId, ptr);
			primaryCacheHandler.addIfAbsent(b);
			SerializedBlock ref = secondaryResourceProvider.getResource(ptr, SerializedBlock.LEAF_BLOCK, System.currentTimeMillis());
			SerializedRecordLeafBlock srlb = new SerializedRecordLeafBlock(ref, true);
			srlb.setNextPtr(null);
			srlb.setPrevPtr(null);
			
			primaryResourceProvider.getResource(ptr, 1);
			secondaryCacheHandler.addIfAbsent(ref);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return b;
	}		
	
	public void bootstrapSchemaBlocks() {
		RecordBlock b = null;
		Set<BlockPtr> pinnedBlocks = new HashSet<BlockPtr>();
		try {
			for (int schemaId = 0; schemaId < 3; schemaId++) {
				try {
					long posn = schemaId * WonderDBServer.DEFAULT_BLOCK_SIZE;
					BlockPtr ptr = new SingleBlockPtr((byte) 0, posn);
					b = new DiskRecordBlock(schemaId, ptr);
					CacheEntryPinner.getInstance().pin(ptr, pinnedBlocks);
					primaryCacheHandler.addIfAbsent(b);
					SerializedBlock ref = secondaryResourceProvider.getResource(ptr, SerializedBlock.LEAF_BLOCK, System.currentTimeMillis());
					SerializedRecordLeafBlock srlb = new SerializedRecordLeafBlock(ref, true);
					srlb.setNextPtr(null);
					srlb.setPrevPtr(null);
					primaryResourceProvider.getResource(ptr, 1);
					secondaryCacheHandler.addIfAbsent(ref);
					secondaryCacheHandler.changed(ptr);
					CollectionTailMgr.getInstance().bootstrap(schemaId, ptr);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}		
			
			CollectionTailMgr.getInstance().bootstrapSerialize(0);
			CollectionTailMgr.getInstance().bootstrapSerialize(1);
			CollectionTailMgr.getInstance().bootstrapSerialize(2);
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
}
