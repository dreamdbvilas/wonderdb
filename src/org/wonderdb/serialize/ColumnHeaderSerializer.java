package org.wonderdb.serialize;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.ColumnHeader;

public class ColumnHeaderSerializer {
	private static ColumnHeaderSerializer instance = new ColumnHeaderSerializer();
	private ColumnHeaderSerializer() {
	}
	
	public static ColumnHeaderSerializer getInstance() {
		return instance;
	}
	
	public ColumnHeader getHeader(ChannelBuffer buffer) {
		int extendBitMask = 0x20;
		int nullBitMask = 0x40;

		byte b = buffer.readByte();
		
		ColumnHeader header = new ColumnHeader();
		
		header.setExtended((b & extendBitMask) > 0 ? true : false);
		header.setNull((b & nullBitMask) > 0 ? true : false);
		
		return header;
	}
	
	public void serialize(ColumnHeader header, ChannelBuffer buffer) {
		byte extendBitMask = 0x20;
		byte nullBitMask = 0x40;

		byte b = 0;
		b = header.isExtended() ? extendBitMask : b;
		b = header.isNull() ? (byte) (b | nullBitMask) : b;

		buffer.writeByte(b);
	}
}
