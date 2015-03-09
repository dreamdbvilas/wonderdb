package org.wonderdb.file;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.Future;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.cache.CacheDestinationWriter;
import org.wonderdb.types.BlockPtr;

public class FileCacheWriter implements CacheDestinationWriter<BlockPtr, ChannelBuffer> {
	ByteBuffer buffer = null;

	@Override
	public void copy(ChannelBuffer data) {
		ChannelBuffer b = ChannelBuffers.copiedBuffer(data);
		b.clear();
		b.writerIndex(b.capacity());
		buffer = b.toByteBuffer();
	}

	@Override
	public Future<Integer> write(BlockPtr ptr) {
		AsynchronousFileChannel channel = null;
		Future<Integer> future = null;
		try {
			channel = FilePointerFactory.getInstance().getAsyncChannel(ptr.getFileId());
			future = channel.write(buffer, ptr.getBlockPosn());
		} finally {
			FilePointerFactory.getInstance().returnAsyncChannel(ptr.getFileId(), channel);
		}
		return future;
	}
}
