package org.wonderdb.serialize.metadata;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.serialize.Serializer;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.serialize.TypeSerializer;
import org.wonderdb.types.DBType;
import org.wonderdb.types.FileBlockEntry;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;

public class FileBlockEntrySerializer implements TypeSerializer {
	public static final FileBlockEntry NULL_FILE_BLOCK_ENTRY = new FileBlockEntry();
	
	private static FileBlockEntrySerializer instance = new FileBlockEntrySerializer();
	
	private FileBlockEntrySerializer() {
	}

	public static FileBlockEntrySerializer getInstance() {
		return instance;
	}
	
	@Override
	public DBType unmarshal(int type, ChannelBuffer buffer, TypeMetadata meta) {
		FileBlockEntry fbe = new FileBlockEntry();
		byte b = buffer.readByte();
		fbe.setFileId(b);
		StringType st = (StringType) Serializer.getInstance().getObject(SerializerManager.STRING, buffer, null);
		String fileName = st.get();
		b = buffer.readByte();
		boolean isDefaultFile = b > 0 ? true : false;
		int blockSize = buffer.readInt();
		
		fbe.setBlockSize(blockSize);
		fbe.setDefaultFile(isDefaultFile);
		fbe.setFileName(fileName);
		return fbe;
	}

	@Override
	public void toBytes(DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		FileBlockEntry fbe = (FileBlockEntry) object;
		buffer.writeByte(fbe.getFileId());
		Serializer.getInstance().serialize(SerializerManager.STRING, new StringType(fbe.getFileName()), buffer, null);
		buffer.writeByte(fbe.isDefaultFile() ? 1 : 0);
		buffer.writeInt(fbe.getBlockSize());
	}

	@Override
	public int getSize(DBType object, TypeMetadata meta) {
		FileBlockEntry fbe = (FileBlockEntry) object;
		int size = 1 + 1 + Integer.SIZE/8;
		size = size + Serializer.getInstance().getObjectSize(SerializerManager.STRING, new StringType(fbe.getFileName()), null);
		return size;
	}

	@Override
	public boolean isNull(int type, DBType object) {
		return object == null || NULL_FILE_BLOCK_ENTRY == object;
	}

	@Override
	public DBType getNull(int type) {
		return NULL_FILE_BLOCK_ENTRY;
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
