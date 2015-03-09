package org.wonderdb.serialize.metadata;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.TypeSerializer;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.DBType;
import org.wonderdb.types.IndexNameMeta;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;

public class IndexNameMetaSerializer implements TypeSerializer {
	
	private static final IndexNameMetaSerializer instance = new IndexNameMetaSerializer();
	
	private IndexNameMetaSerializer() {
	}
	
	public static final IndexNameMetaSerializer getInstance() {
		return instance;
	}

	@Override
	public DBType unmarshal(int type, ChannelBuffer buffer, TypeMetadata meta) {
		int id = buffer.readInt();
		String indexName = ((StringType) Serializer.getInstance().getObject(SerializerManager.STRING, buffer, meta)).get();
		String collectionName = ((StringType) Serializer.getInstance().getObject(SerializerManager.STRING, buffer, meta)).get();
		
		int size = buffer.readInt();
		List<Integer> columnIdList = new ArrayList<Integer>(size);
		for (int i = 0; i < size; i++) {
			int t = buffer.readInt();
			columnIdList.add(t);
		}
		BlockPtr head = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, meta);
		byte b = buffer.readByte();
		byte b1 = buffer.readByte();
		
		IndexNameMeta inm = new IndexNameMeta();
		inm.setCollectionName(collectionName);
		inm.setColumnIdList(columnIdList);
		inm.setHead(head);
		inm.setId(id);
		inm.setIndexName(indexName);
		inm.setUnique(b > 0 ? true : false);
		inm.setAscending(b1 > 0 ? true : false);
		
		return inm;
	}

	@Override
	public void toBytes(DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		IndexNameMeta inm = (IndexNameMeta) object;
		
		buffer.writeInt(inm.getId());
		Serializer.getInstance().serialize(SerializerManager.STRING, new StringType(inm.getIndexName()), buffer, meta);
		Serializer.getInstance().serialize(SerializerManager.STRING, new StringType(inm.getCollectionName()), buffer, meta);
		buffer.writeInt(inm.getColumnIdList().size());
		for (int i = 0; i < inm.getColumnIdList().size(); i++) {
			int type = inm.getColumnIdList().get(i);
			buffer.writeInt(type);
		}
		Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, inm.getHead(), buffer, meta);
		buffer.writeByte(inm.isUnique() ? (byte) 1 : (byte) 0);
		buffer.writeByte(inm.isAscending() ? 1 : 0);
	}

	@Override
	public int getSize(DBType object, TypeMetadata meta) {
		IndexNameMeta inm = (IndexNameMeta) object;
		int size = Integer.BYTES;
		size = size + Serializer.getInstance().getObjectSize(SerializerManager.STRING, new StringType(inm.getCollectionName()), meta);
		size = size + Serializer.getInstance().getObjectSize(SerializerManager.STRING, new StringType(inm.getIndexName()), meta);
		size = size + Integer.BYTES;
		size = size + inm.getColumnIdList().size()*Integer.BYTES;
		size = size + Serializer.getInstance().getObjectSize(SerializerManager.BLOCK_PTR, inm.getHead(), meta);
		size = size + 1;
		size = size + 1;
		
		return size;
	}

	@Override
	public boolean isNull(int type, DBType object) {
		return false;
	}

	@Override
	public DBType getNull(int type) {
		return null;
	}

	@Override
	public int getSQLType(int type) {
		throw new RuntimeException("Method not supported");
	}

	@Override
	public DBType convert(int type, StringType st) {
		throw new RuntimeException("Method not supported");
	}
}
