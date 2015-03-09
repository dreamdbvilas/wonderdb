package org.wonderdb.freeblock;

import org.wonderdb.types.BlockPtr;

public interface FreeBlockMgr {
	long getNextPosn(byte filedId);
	void free(BlockPtr ptr);
}
