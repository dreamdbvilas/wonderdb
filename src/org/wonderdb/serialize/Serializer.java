package org.wonderdb.serialize;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.TypeMetadata;

public class Serializer {
	private static Serializer instance = new Serializer();
	
	private Serializer() {
	}
	
	public static Serializer getInstance() {
		return instance;
	}
	
	public DBType getObject(int type, ChannelBuffer buffer, TypeMetadata meta) {
		byte b = buffer.readByte();
		if ((b & 0x01) > 0) {
			return SerializerManager.getInstance().getSerializer(type).getNull(type);
		}
		return SerializerManager.getInstance().getSerializer(type).unmarshal(type, buffer, meta);
	}
	
	public void serialize(int type, DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		if (SerializerManager.getInstance().getSerializer(type).isNull(type, object)) {
			buffer.writeByte(1);
		} else {
			buffer.writeByte(0);
			SerializerManager.getInstance().getSerializer(type).toBytes(object, buffer, meta);
		}
	}
	
	public int getObjectSize(int type, DBType object, TypeMetadata meta) {
		return 1+SerializerManager.getInstance().getSerializer(type).getSize(object, meta);
	}
	
	public int getSQLType(int type) {
		return SerializerManager.getInstance().getSerializer(type).getSQLType(type);
	}
}
