package org.wonderdb.serialize;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;

public interface TypeSerializer {
	public abstract DBType unmarshal(int type, ChannelBuffer buffer, TypeMetadata meta);
	public abstract void toBytes(DBType object, ChannelBuffer buffer, TypeMetadata meta);
	public abstract int getSize(DBType object, TypeMetadata meta);
	public abstract boolean isNull(int type, DBType object);
	public abstract DBType getNull(int type);
	public abstract int getSQLType(int type);
	public abstract DBType convert(int type, StringType st);
}
