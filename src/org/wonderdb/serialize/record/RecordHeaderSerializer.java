package org.wonderdb.serialize.record;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.record.RecordHeader;

public class RecordHeaderSerializer {
	private static RecordHeaderSerializer instance = new RecordHeaderSerializer();
	private RecordHeaderSerializer() {
	}
	
	public static RecordHeaderSerializer getInstance() {
		return instance;
	}
	
	public RecordHeader getHeader(ChannelBuffer buffer) {
		byte extendedBitMask = 0x1;
		
		RecordHeader header = new RecordHeader();
		byte b = buffer.readByte();
		header.setExtended((b & extendedBitMask) > 0 ? true : false);
		return header;
	}
	
	public void serialize(RecordHeader header, ChannelBuffer buffer) {
		buffer.writeByte(header.isExtended() ? 1 : 0);
	}
}
