package org.wonderdb.serialize.metadata;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.TypeSerializer;
import org.wonderdb.types.ColumnNameMeta;
import org.wonderdb.types.DBType;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;

public class ColumnNameMetaSerializer implements TypeSerializer {
	public static final ColumnNameMetaSerializer instance = new ColumnNameMetaSerializer();
	
	private ColumnNameMetaSerializer() {
	}
	
	public static final ColumnNameMetaSerializer getInstance() {
		return instance;
	}
	
	@Override
	public DBType unmarshal(int type, ChannelBuffer buffer, TypeMetadata meta) {
		int coulmnId = buffer.readInt();
		String columnName = ((StringType) Serializer.getInstance().getObject(SerializerManager.STRING, buffer, meta)).get();
		String collectioName = ((StringType) Serializer.getInstance().getObject(SerializerManager.STRING, buffer, meta)).get();
		int columnType = buffer.readInt();
		boolean isNull = buffer.readByte() > 0 ? true : false;
		boolean isVirtual = buffer.readByte() > 0 ? true : false;
		
		ColumnNameMeta cnm = new ColumnNameMeta();
		cnm.setCollectioName(collectioName);
		cnm.setColumnName(columnName);
		cnm.setColumnType(coulmnId);
		cnm.setNull(isNull);
		cnm.setColumnType(columnType);
		cnm.setVirtual(isVirtual);
		return cnm;
	}

	@Override
	public void toBytes(DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		ColumnNameMeta cnm = (ColumnNameMeta) object;
		
		buffer.writeInt(cnm.getCoulmnId());
		Serializer.getInstance().serialize(SerializerManager.STRING, new StringType(cnm.getColumnName()), buffer, meta);
		Serializer.getInstance().serialize(SerializerManager.STRING, new StringType(cnm.getCollectioName()), buffer, meta);
		buffer.writeInt(cnm.getColumnType());
		buffer.writeByte(cnm.isNull() ? 1 : 0);
		buffer.writeByte(cnm.isVirtual() ? 1 : 0);
	}

	@Override
	public int getSize(DBType object, TypeMetadata meta) {
		ColumnNameMeta cnm = (ColumnNameMeta) object;

		int size = Integer.SIZE/8;
		size = size + Serializer.getInstance().getObjectSize(SerializerManager.STRING, new StringType(cnm.getColumnName()), meta);
		size = size + Serializer.getInstance().getObjectSize(SerializerManager.STRING, new StringType(cnm.getCollectioName()), meta);
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
