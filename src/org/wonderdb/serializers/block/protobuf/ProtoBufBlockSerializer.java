package org.wonderdb.serializers.block.protobuf;


public class ProtoBufBlockSerializer {
	/*
	private static ProtoBufBlockSerializer instance = new ProtoBufBlockSerializer();
	private static CacheHandler<BlockPtr, ChannelBuffer> secondaryCacheHandler = SecondaryCacheHandlerFactory.getInstance().getCacheHandler();

	private ProtoBufBlockSerializer() {
	}
	
	public static ProtoBufBlockSerializer getInstance() {
		return instance;
	}
	
	private ProtoBlockPtr buildProtoBlockPtr(BlockPtr ptr) {
		if (ptr == null) {
			return null;
		}
		
		byte[] bytes = new byte[1];
		bytes[0] = ptr.getFileId();
		ByteString bs = ByteString.copyFrom(bytes);
		ProtoBlockPtr p = ProtoBlockPtr.newBuilder().setFileId(bs).setPosn(ptr.getBlockPosn()).build();
		return p;
	}
	
	private void buildDbType(DBType dt, ProtoColumn.Builder pcBuilder) {
		if (dt == null) {
			return;
		}
		if (dt instanceof StringType) {
			pcBuilder.setStrVal(((StringType) dt).get());
		}
		if (dt instanceof IntType) {
			pcBuilder.setIntVal(((IntType) dt).get());
		}
		if (dt instanceof LongType) {
			pcBuilder.setLongVal(((LongType) dt).get());
		}
		if (dt instanceof DoubleType) {
			pcBuilder.setDoubleVale(((DoubleType) dt).get());
		}
		if (dt instanceof FloatType) {
			pcBuilder.setFloatVal(((FloatType) dt).get());
		}
		if (dt instanceof StringType) {
			pcBuilder.setStrVal(((StringType) dt).get());
		}
		if (dt instanceof CollectionNameMeta) {
			pcBuilder.setCollectionNameMeta(buildProtoCollectionNameMeta((CollectionNameMeta) dt));
		}
		if (dt instanceof ColumnNameMeta) {
			pcBuilder.setColumnNameMeta(buildColumnNameMeta((ColumnNameMeta) dt));
		}
		if (dt instanceof IndexNameMeta) {
			pcBuilder.setIndexNameMeta(buildIndexNameMeta((IndexNameMeta) dt));
		}
		if (dt instanceof FileBlockEntry) {
			pcBuilder.setFileBlockEntry(buildFileBlockEntry((FileBlockEntry) dt));
		}
		if (dt instanceof IndexKeyType) {
			pcBuilder.setIndexKeyType(buildIndexKeyType((IndexKeyType) dt));
		}
	}
	
	private ProtoIndexKeyType buildIndexKeyType(IndexKeyType key) {
		ProtoIndexKeyType.Builder builder = ProtoIndexKeyType.newBuilder();
		
		for (int i = 0; i < key.getValue().size(); i++) {
			Column column = new Column(key.getValue().get(i));
			builder.setColumn(i, buildProtoColumn(column));
		}
		builder.setRecordId(buildProtoReordId(key.getRecordId()));
		
		return builder.build();
	}
	
	private ProtoFileBlockEntry buildFileBlockEntry(FileBlockEntry entry) {
		ProtoFileBlockEntry.Builder builder = ProtoFileBlockEntry.newBuilder();
		
		builder.setBlockSize(entry.getBlockSize())
		.setDefaultFile(entry.isDefaultFile())
		.setFileName(entry.getFileName())
		.setRecordId(buildProtoReordId(entry.getRecordId()));
		
		byte[] bytes = new byte[1];
		bytes[0] = entry.getFileId();
		builder.setFileId(ByteString.copyFrom(bytes));
		
		return builder.build();
	}
	
	private ProtoIndexNameMeta buildIndexNameMeta(IndexNameMeta meta) {
		ProtoIndexNameMeta.Builder builder = ProtoIndexNameMeta.newBuilder();
		
		builder.setCollectionName(meta.getCollectionName());
		for (int i = 0; i < meta.getColumnIdList().size(); i++) {
			builder.setColumnIdList(i, meta.getColumnIdList().get(i));
		}
		builder.setHead(buildProtoBlockPtr(meta.getHead()))
		.setIndexName(meta.getIndexName())
		.setRecordId(buildProtoReordId(meta.getRecordId()))
		.setUnique(meta.isUnique())
		.setIsAscending(meta.isAscending());
		
		return builder.build();
	}
	
	private ProtoColumnNameMeta buildColumnNameMeta(ColumnNameMeta meta) {
		ProtoColumnNameMeta.Builder builder = ProtoColumnNameMeta.newBuilder();
		
		builder.setCollectioName(meta.getCollectioName())
		.setColumnName(meta.getColumnName())
		.setColumnType(meta.getColumnType())
		.setCoulmnId(meta.getCoulmnId())
		.setRecId(buildProtoReordId(meta.getRecId()));
		
		return builder.build();
	}
	
	private ProtoRecordId buildProtoReordId(RecordId recId) {
		ProtoRecordId.Builder builder = ProtoRecordId.newBuilder();
		builder.setBlockPtr(buildProtoBlockPtr(recId.getPtr()))
		.setPosn(recId.getPosn());
		
		return builder.build();
	}
	
	private ProtoCollectionNameMeta buildProtoCollectionNameMeta(CollectionNameMeta meta) {
		ProtoCollectionNameMeta.Builder pcnmBuilder = ProtoCollectionNameMeta.newBuilder();
		pcnmBuilder.setConcurrency(meta.getConcurrency())
		.setHead(buildProtoBlockPtr(meta.getHead()))
		.setIsLoggingEnabled(meta.isLoggingEnabled())
		.setName(meta.getName())
		.setRecordId(buildProtoReordId(meta.getRecordId()));
		
		return pcnmBuilder.build();
	}
	
	private ProtoColumn buildProtoColumn(Column column) {
		ProtoColumn.Builder pcBuilder = ProtoColumn.newBuilder();
		if (column instanceof Extended) {
			BlockPtr ptr = ((Extended) column).getPtrList().get(0);
			ProtoBlockPtr pbp = buildProtoBlockPtr(ptr);
			pcBuilder.setPtr(pbp);
			pcBuilder.setIsExtended(true);
			return pcBuilder.build();
		}
		
		DBType dt = column.getValue();
		buildDbType(dt, pcBuilder);
		return pcBuilder.build();
	}
	
	private ProtoRecord buildProtoRecord(Record record) {
		ProtoRecord.Builder builder = ProtoRecord.newBuilder();
		
		if (record instanceof ObjectListRecord) {
			Column column =  ((ObjectListRecord) record).getColumn();
			ProtoColumn pColumn = buildProtoColumn(column);
			builder.setColumns(0, pColumn);
		}
		
		if (record instanceof ListRecord) {
			ListRecord lr = (ListRecord) record;
			for (int i = 0; i < lr.)
		}
		return null;
	}
	
	private ProtoBlock buildProtoBlock(Block block) {
		ProtoBlock.Builder pBlockBuilder = ProtoBlock.newBuilder();
		
		if (block instanceof IndexBlock) {
			ProtoBlockPtr protoParent = buildProtoBlockPtr(block.getParent());
			pBlockBuilder.setParent(protoParent);
		} 
		
		if (block instanceof IndexLeafBlock || block instanceof ListBlock) {
			ProtoBlockPtr protoNext = buildProtoBlockPtr(block.getNext());
			ProtoBlockPtr protoPrev = buildProtoBlockPtr(block.getPrev());
			pBlockBuilder.setNext(protoNext).setPrev(protoPrev);
			pBlockBuilder.setPrev(protoPrev);
		}
		
		if (block instanceof ListBlock) {
			pBlockBuilder.setMaxPosn(((ListBlock) block).getMaxPosn());
		}
		
		List<Record> recList = block.getData();
		for (int i = 0; i < recList.size(); i++) {
			Record record = recList.get(i);
			
		}
		return pBlockBuilder.build();
	}
	
	public void serialize(Block block, TransactionId id) {
		SerializedBlockImpl serializedBlock = (SerializedBlockImpl) secondaryCacheHandler.get(block.getPtr());
		serializedBlock.setLastAccessTime(block.getLastAccessTime());
		ChannelBuffer buffer = serializedBlock.getData();
		buffer.clear();

		int size = buffer.readInt();
		byte[] bytes = new byte[size];
		buffer.readBytes(bytes);
		ProtoBlock pBlock = Wonderdb.ProtoBlock.parseFrom(bytes);
		BlockType blockType = pBlock.getType();
		
		BlockHeader header = getBlockHeader(block);
		BlockHeaderSerializer.getInstance().serialize(header, buffer);
		
		Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, block.getNext(), buffer, null);
		Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, block.getPrev(), buffer, null);
		buffer.writeInt(0); //reserved
		
		if (header.isIndexBlock() || header.isIndexBranchBlock()) {
			Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, block.getParent(), buffer, meta);
		} else {
			int maxPosn = ((ListBlock) block).getMaxPosn();
			buffer.writeInt(maxPosn);
		}

		List<Record> list = block.getData();
		buffer.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			TypeMetadata newMeta = meta;
			Record record = list.get(i);
			if (record instanceof ListRecord) {
				RecordId recordId = ((ListRecord) record).getRecordId();
				buffer.writeInt(recordId.getPosn());
			}
			RecordSerializer.getInstance().serializeMinimum(record, buffer, newMeta);
		}
		secondaryCacheHandler.changed(block.getPtr());
		LogManager.getInstance().logBlock(id, serializedBlock);

	}
	*/
}
