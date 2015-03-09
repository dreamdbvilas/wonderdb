package org.wonderdb.types.record;

import java.util.ArrayList;
import java.util.List;

import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.Extended;

public class ExtendedIndexRecord extends IndexRecord implements Extended {
	List<BlockPtr> ptrList = new ArrayList<BlockPtr>();

	public List<BlockPtr> getPtrList() {
		return ptrList;
	}

	public void setPtrList(List<BlockPtr> ptrList) {
		this.ptrList = ptrList;
	}
}
