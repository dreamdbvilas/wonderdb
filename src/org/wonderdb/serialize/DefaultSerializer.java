package org.wonderdb.serialize;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.BlockPtrList;
import org.wonderdb.types.ByteArrayType;
import org.wonderdb.types.DBType;
import org.wonderdb.types.DoubleType;
import org.wonderdb.types.FloatType;
import org.wonderdb.types.IntType;
import org.wonderdb.types.LongType;
import org.wonderdb.types.SingleBlockPtr;
import org.wonderdb.types.StringType;
import org.wonderdb.types.TypeMetadata;

public class DefaultSerializer implements TypeSerializer {
	public static DefaultSerializer instance = new DefaultSerializer();

	public static final StringType NULL_STRING = new StringType(null);
	public static final IntType NULL_INT = new IntType(null);
	public static final LongType NULL_LONG = new LongType(null);
	public static final DoubleType NULL_DOUBLE = new DoubleType(null);
	public static final FloatType NULL_FLOAT = new FloatType(null);
	public static final BlockPtr NULL_BLKPTR = new SingleBlockPtr((byte) -1, -1);
	public static final BlockPtrList NULL_BLKPTRLIST = new BlockPtrList(null);
	public static final ByteArrayType NULL_BYTE_ARRAY = new ByteArrayType(null);
	
	private DefaultSerializer() {
	}

	public static DefaultSerializer getInstance() {
		return instance;
	}
	
	@Override
	public DBType unmarshal(int type, ChannelBuffer buffer, TypeMetadata meta) {
		switch (type) {
		case SerializerManager.STRING:
		case SerializerManager.BYTE_ARRAY_TYPE:
			int size = buffer.readInt();
			byte[] bytes = new byte[size];
			buffer.readBytes(bytes);
			if (type==SerializerManager.STRING) {
				return new StringType(new String(bytes));
			} else {
				return new ByteArrayType(bytes);
			}
		case SerializerManager.INT:
			return new IntType(buffer.readInt());
		case SerializerManager.LONG:
			return new LongType(buffer.readLong());
		case SerializerManager.DOUBLE:
			return new DoubleType(buffer.readDouble());
		case SerializerManager.FLOAT:
			return new FloatType(buffer.readFloat());
		case SerializerManager.BLOCK_PTR:
			byte fileId = buffer.readByte();
			long posn = buffer.readLong();
			if (fileId < 0 || posn < 0) {
				return null;
			}
			return new SingleBlockPtr(fileId, posn);
		case SerializerManager.BLOCK_PTR_LIST_TYPE:
			int s = buffer.readInt();
			List<BlockPtr> list = new ArrayList<BlockPtr>(s);
			for (int i = 0; i < s; i++) {
				BlockPtr ptr = (BlockPtr) unmarshal(SerializerManager.BLOCK_PTR, buffer, null);
				list.add(ptr);
			}
			return new BlockPtrList(list);
			default:
				return null;
		}
	}

	@Override
	public void toBytes(DBType object, ChannelBuffer buffer, TypeMetadata meta) {
		if (object instanceof StringType) {
			StringType s = (StringType) object;
			buffer.writeInt(s.get().getBytes().length);
			buffer.writeBytes(s.get().getBytes());
			return;
		}

		if (object instanceof ByteArrayType) {
			ByteArrayType s = (ByteArrayType) object;
			buffer.writeInt(s.get().length);
			buffer.writeBytes(s.get());
			return;
		}
		
		if (object instanceof IntType) {
			buffer.writeInt(((IntType) object).get());
			return;
		}
		
		if (object instanceof LongType) {
			buffer.writeLong(((LongType) object).get());
			return;
		}
		
		if (object instanceof DoubleType) {
			buffer.writeDouble(((DoubleType) object).get());
			return;
		}
		
		if (object instanceof FloatType) {
			buffer.writeFloat(((FloatType) object).get());
			return;
		}
		
		if (object instanceof BlockPtr) {
			BlockPtr p = (BlockPtr) object;
			buffer.writeByte(p.getFileId());
			buffer.writeLong(p.getBlockPosn());
			return;
		}
		
		if (object instanceof BlockPtrList) {
			BlockPtrList list = (BlockPtrList) object;
			buffer.writeInt(list.getPtrList().size());
			for (int i = 0; i < list.getPtrList().size(); i++) {
				BlockPtr ptr = list.getPtrList().get(i);
				DefaultSerializer.getInstance().toBytes(ptr, buffer, null);
			}
		}
	}

	@Override
	public int getSize(DBType object, TypeMetadata meta) {
		if (object instanceof StringType) {
			String value = ((StringType) object).get();
			return  1 + value.getBytes().length + Integer.SIZE/8;
		} 
		
		if (object instanceof ByteArrayType) {
			byte[] array = ((ByteArrayType) object).get();
			return 1 + array.length + Integer.SIZE/8;
		}

		if (object instanceof BlockPtr) {
			return 1+ 1 + Long.SIZE/8; 
		}
		
		return 1+Long.SIZE/8;
	}

	@Override
	public boolean isNull(int type, DBType object) {
		switch (type) {
		case SerializerManager.STRING:
			return NULL_STRING.equals(object) || object == null;
		case SerializerManager.BYTE_ARRAY_TYPE:
			return NULL_BYTE_ARRAY.equals(object);
		case SerializerManager.INT:
			return NULL_INT.equals(object) || object == null;
		case SerializerManager.LONG:
			return NULL_LONG.equals(object) || object == null;
		case SerializerManager.DOUBLE:
			return NULL_DOUBLE.equals(object) || object == null;
		case SerializerManager.FLOAT:
			return NULL_FLOAT.equals(object) || object == null;
		case SerializerManager.BLOCK_PTR:
			return NULL_BLKPTR.equals(object) || object == null;
		case SerializerManager.BLOCK_PTR_LIST_TYPE:
			return NULL_BLKPTRLIST.equals(object) || object == null;
			default:
				return true;
		}
	}

	@Override
	public DBType getNull(int type) {
		return null;
//		switch (type) {
//		case SerializerManager.STRING:
//			return NULL_STRING;
//		case SerializerManager.INT:
//			return NULL_INT;
//		case SerializerManager.LONG:
//			return NULL_LONG;
//		case SerializerManager.DOUBLE:
//			return NULL_DOUBLE;
//		case SerializerManager.FLOAT:
//			return NULL_FLOAT;
//		case SerializerManager.BLOCK_PTR:
//			return NULL_BLKPTR;
//		case SerializerManager.BLOCK_PTR_LIST_TYPE:
//			return NULL_BLKPTRLIST;
//			default:
//				return null;
//		}		
	}

	@Override
	public int getSQLType(int type) {
		switch (type) {
		case SerializerManager.STRING: return Types.VARCHAR;
		case SerializerManager.DOUBLE: return Types.DOUBLE;
		case SerializerManager.FLOAT: return Types.FLOAT;
		case SerializerManager.INT: return Types.INTEGER;
		case SerializerManager.LONG: return Types.NUMERIC;
		case SerializerManager.BYTE_ARRAY_TYPE: return Types.BINARY;
		default: return -1;
		}
	}

	@Override
	public DBType convert(int type, StringType st) {
		switch (type) {
			case SerializerManager.STRING: return st;
			case SerializerManager.DOUBLE: 
				if (st == NULL_STRING) {
					return NULL_DOUBLE;
				}
				return new DoubleType(Double.parseDouble(st.get()));
			case SerializerManager.FLOAT: 
				if (st == NULL_STRING) {
					return NULL_FLOAT;
				}
				return new FloatType(Float.parseFloat(st.get()));
			case SerializerManager.INT: 
				if (st == NULL_STRING) {
					return NULL_FLOAT;
				}
				return new IntType(Integer.parseInt(st.get()));
			case SerializerManager.LONG:
				if (st == NULL_STRING) {
					return NULL_FLOAT;
				}
				return new LongType(Long.parseLong(st.get()));
			default: return null;
		}
	}
}
