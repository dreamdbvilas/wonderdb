package org.wonderdb.file;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.Future;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.CacheDestinationWriter;
import org.wonderdb.types.BlockPtr;

public class FileCacheWriter implements CacheDestinationWriter<BlockPtr, ChannelBuffer> {

	@Override
	public ChannelBuffer copy(BlockPtr ptr, ChannelBuffer data) {
		
		data.clear();
		data.writerIndex(data.capacity());
		ChannelBuffer b = ChannelBuffers.copiedBuffer(data);
		return b;
//		data.clear();
//		data.writerIndex(data.capacity());
//		ChannelBuffer b = ChannelBuffers.copiedBuffer(data);
//		b.clear();
//		b.writerIndex(b.capacity());
//		buffer = b.toByteBuffer();
	}

	@Override
	public Future<Integer> write(BlockPtr p, ChannelBuffer data) {
		AsynchronousFileChannel channel = null;
		data.clear();
		data.writerIndex(data.capacity());
		ByteBuffer buf = data.toByteBuffer();
		channel = FilePointerFactory.getInstance().getAsyncChannel(p.getFileId());
		return channel.write(buf, p.getBlockPosn());
	}
}
