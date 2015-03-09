package org.wonderdb.metadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.wonderdb.cache.impl.CacheEntryPinner;
import org.wonderdb.core.collection.ResultIterator;
import org.wonderdb.core.collection.WonderDBList;
import org.wonderdb.file.FilePointerFactory;
import org.wonderdb.freeblock.FreeBlockFactory;
import org.wonderdb.serialize.SerializerManager;
import org.wonderdb.server.WonderDBPropertyManager;
import org.wonderdb.storage.FileBlockManager;
import org.wonderdb.txnlogger.LogManager;
import org.wonderdb.txnlogger.TransactionId;
import org.wonderdb.types.BlockPtr;
import org.wonderdb.types.ColumnSerializerMetadata;
import org.wonderdb.types.DBType;
import org.wonderdb.types.FileBlockEntry;
import org.wonderdb.types.SingleBlockPtr;
import org.wonderdb.types.record.ListRecord;
import org.wonderdb.types.record.ObjectListRecord;

public class StorageMetadata {
	private Map<Byte, FileBlockEntry> fileIdToEntryMap = new ConcurrentHashMap<>();
	private Map<String, FileBlockEntry> fileNameToEntryMap = new ConcurrentHashMap<>();
	private WonderDBList storageList = null;
	private boolean initialized = false;
	
	private byte defaultFileId = 0;
	private static final StorageMetadata instance = new StorageMetadata();
	
	private StorageMetadata() {
	}
	
	public void init(boolean isNew) {
		
		if (isNew) {
			create();
		} else {
			load();
		}
		StorageMetadata.getInstance().initialized = true;
	}
	
	public void add(FileBlockEntry entry) {
		if (!initialized) {
			throw new RuntimeException("Please initialize StorageMetadata by calling StorageMetadata.init(..)");
		}

		TransactionId txnId = LogManager.getInstance().startTxn();
		Set<Object> pinnedBlocks = new HashSet<>();
		
		try {
			entry.setFileId((byte) fileIdToEntryMap.size());
			addMeta(entry);
			ObjectListRecord record = new ObjectListRecord(entry);
			storageList.add(record, txnId, new ColumnSerializerMetadata(SerializerManager.FILE_BLOCK_ENRTY_TYPE), pinnedBlocks);
			
			if (entry.isDefaultFile()) {
				FileBlockEntry defEntry = fileIdToEntryMap.get(defaultFileId);
				defEntry.setDefaultFile(false);
				if (defEntry != null && defEntry.getFileId() != 0) {
					ListRecord rec = new ObjectListRecord(defEntry);
					storageList.update(rec, rec, txnId, new ColumnSerializerMetadata(SerializerManager.FILE_BLOCK_ENRTY_TYPE), pinnedBlocks);
				}
				defaultFileId = entry.getFileId();
			}
		} finally {
			LogManager.getInstance().commitTxn(txnId);
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public List<FileBlockEntry> getStorageEntries() {
		if (!initialized) {
			throw new RuntimeException("Please initialize StorageMetadata by calling StorageMetadata.init(..)");
		}

		return new ArrayList<FileBlockEntry>(fileIdToEntryMap.values());
	}
	
	public String getDefaultFileName() {
		FileBlockEntry entry = fileIdToEntryMap.get(defaultFileId);
		return entry.getFileName();
	}
	
	public void shutdown() {
		FilePointerFactory.getInstance().closAll();
	}
	
	private void create() {
		Set<Object> pinnedBlocks = new HashSet<>();
		try {
			FileBlockEntry entry = getDefaultFileBlockEntry();
			addMeta(entry);
			BlockPtr ptr = new SingleBlockPtr((byte) 0, 0);
			storageList = WonderDBList.create("_storage", ptr, 1, null, pinnedBlocks);
			defaultFileId = 0;
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	private FileBlockEntry getDefaultFileBlockEntry() {
		String systemFileName = WonderDBPropertyManager.getInstance().getSystemFile();
		int defaultBlockSize = WonderDBPropertyManager.getInstance().getDefaultBlockSize();
		FileBlockEntry entry = new FileBlockEntry();
		entry.setBlockSize(defaultBlockSize);
//		entry.setDefaultFile(true);
		entry.setFileId((byte) 0);
		entry.setFileName(systemFileName);
		return entry;
	}
	
	private void load() {
		Set<Object> pinnedBlocks = new HashSet<>();
		FileBlockEntry entry = null;
		try {
			entry = getDefaultFileBlockEntry();
			addMeta(entry);
			BlockPtr ptr = new SingleBlockPtr((byte) 0, 0);
			storageList = WonderDBList.load("_storage", ptr, 1, new ColumnSerializerMetadata(SerializerManager.BLOCK_PTR_LIST_TYPE), pinnedBlocks);
			ResultIterator iter = storageList.iterator(new ColumnSerializerMetadata(SerializerManager.FILE_BLOCK_ENRTY_TYPE), pinnedBlocks);
			try {
				while (iter.hasNext()) {
					ObjectListRecord record = (ObjectListRecord) iter.next();
					DBType column = record.getColumn();
					entry = (FileBlockEntry) column;
					if (entry.isDefaultFile()) {
						defaultFileId = entry.getFileId();
					}
					entry.setRecordId(record.getRecordId());
					addMeta(entry);
				}
			} finally {
				iter.unlock(true);
			}
		} finally {
			CacheEntryPinner.getInstance().unpin(pinnedBlocks, pinnedBlocks);
		}
	}
	
	public byte getFileId(String fileName) {
		FileBlockEntry entry = fileNameToEntryMap.get(fileName);
		return entry != null ? entry.getFileId() : -1;
	}
	
	public byte getDefaultFileId() {
		return defaultFileId;
	}

	public void setDefaultFileId(byte defaultFileId) {
		this.defaultFileId = defaultFileId;
	}
	
	public String getFileName(byte fileId) {
		FileBlockEntry entry = fileIdToEntryMap.get(fileId);
		if (entry != null) {
			return entry.getFileName();
		}
		return null;
	}

	private void addMeta(FileBlockEntry entry) {
		FileBlockEntry oldValue = fileNameToEntryMap.putIfAbsent(entry.getFileName(), entry);
		if (oldValue != null) {
			throw new RuntimeException("Entry already present");
		}
		fileIdToEntryMap.put(entry.getFileId(), entry);
		FilePointerFactory.getInstance().create(entry);
		FileBlockManager.getInstance().addFileBlockEntry(entry);
		FreeBlockFactory.getInstance().createNewMgr(entry.getFileId(), 0, 0);
	}
	
	public static final StorageMetadata getInstance() {
		return instance;
	}
}
