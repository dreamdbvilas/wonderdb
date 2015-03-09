package org.wonderdb.types;

import java.util.List;

public interface Extended {
	List<BlockPtr> getPtrList();
	void setPtrList(List<BlockPtr> list);
}
