package org.wonderdb.block.weak;

import java.lang.ref.WeakReference;

import org.wonderdb.block.Block;
import org.wonderdb.types.TypeMetadata;

public class WeakBlock {
	WeakReference<Block> blockRef = null;
	WeakReference<TypeMetadata> metaRef = null;
	
	public WeakBlock(Block b, TypeMetadata m) {
		blockRef = new WeakReference<Block>(b);
		metaRef = new WeakReference<TypeMetadata>(m);
	}
}
