package org.wonderdb.serialize.metadata;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.TypeSerializer;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.CollectionNameMeta;
import org.wonderdb.types.DBType;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;

public class CollectionNameMetaSerializer implements TypeSerializer {
	public static final CollectionNameMetaSerializer instance = new CollectionNameMetaSerializer();
	
	private CollectionNameMetaSerializer() {
	}
	
	public final static CollectionNameMetaSerializer getInstance() {
		return instance;
	}

	@Override
	public DBType unmarshal(int type, ChannelBuffer buffer, TypeMetadata meta) {
		CollectionNameMeta cnm = new CollectionNameMeta();
		String name = (( StringType) Serializer.getInstance().getObject(SerializerManager.STRING, buffer, null)).get();
		BlockPtr ptr = (BlockPtr) Serializer.getInstance().getObject(SerializerManager.BLOCK_PTR, buffer, null);
		int concurrency = buffer.readInt();
		byte b = buffer.readByte();
		
		cnm.setHead(ptr);
		cnm.setName(name);
		cnm.setConcurrency(concurrency);
		cnm.setLoggingEnabled(b > 0 ? true : false);
		return cnm;
	}

	@Override
	public void toBytes(DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		CollectionNameMeta cnm = (CollectionNameMeta) object;
		Serializer.getInstance().serialize(SerializerManager.STRING, new StringType(cnm.getName()), buffer, meta);
		Serializer.getInstance().serialize(SerializerManager.BLOCK_PTR, cnm.getHead(), buffer, meta);	
		buffer.writeInt(cnm.getConcurrency());
		buffer.writeByte(cnm.isLoggingEnabled() ? 1 : 0);
	}

	@Override
	public int getSize(DBType object, TypeMetadata meta) {
		CollectionNameMeta cnm = (CollectionNameMeta) object;		
		int size = Serializer.getInstance().getObjectSize(SerializerManager.STRING, new StringType(cnm.getName()), meta);
		size = size + Serializer.getInstance().getObjectSize(SerializerManager.BLOCK_PTR, cnm.getHead(), meta);
		size = size + Integer.SIZE/8;
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
