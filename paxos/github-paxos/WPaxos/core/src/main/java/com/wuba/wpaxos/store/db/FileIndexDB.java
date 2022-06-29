/*
 * Copyright (C) 2005-present, 58.com.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wuba.wpaxos.store.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.store.DefaultLogStorage;
import com.wuba.wpaxos.store.FileID;
import com.wuba.wpaxos.store.VarStorage;
import com.wuba.wpaxos.store.pagecache.MapedFile;
import com.wuba.wpaxos.store.pagecache.MapedFileQueue;
import com.wuba.wpaxos.store.service.CleanIndexMappedFileService;
import com.wuba.wpaxos.store.service.CommitIndexService;
import com.wuba.wpaxos.store.service.FlushIndexService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store paxoslog index in file
 */
public class FileIndexDB implements IndexDB {
	private static final Logger log = LogManager.getLogger(FileIndexDB.class);

	// Storage unit size[(long)，Offset (int)，Crc32 (int)，Size]
	public static final int CQStoreUnitSize = 16;
	// Queue storing message index
	private final MapedFileQueue mapedFileQueue;
	// instaceID
	private final int groupId;
	private VarStorage varStorage;
	// Configuration
	private final String storePath;
	private final int mapedFileSize;
	private long maxInstanceID = -1;
	private long minInstanceID = -1;
	private static int maxIndexCache = 500000;
	private ConcurrentHashMap<Long, FileID> indexMap;
	private FlushIndexService flushIndexService;
	private CommitIndexService commitIndexService;
	private DefaultLogStorage defaultLogStorage;
	private CleanIndexMappedFileService cleanIndexMappedFileService;

	public FileIndexDB(DefaultLogStorage defaultLogStorage, int groupId, String storePath) {
		super();
		this.defaultLogStorage = defaultLogStorage;
		this.groupId = groupId;
		this.storePath = storePath;
		this.mapedFileSize = defaultLogStorage.getStoreConfig().getMaxIndexNum() * CQStoreUnitSize;
		this.mapedFileQueue = new MapedFileQueue(storePath, mapedFileSize, defaultLogStorage.getAllocateMapedFileService());
		this.indexMap = new ConcurrentHashMap<Long, FileID>(this.mapedFileSize);
	}

	public boolean commit(final int flushLeastPages) {
		this.mapedFileQueue.commit(flushLeastPages);
		return true;
	}

	public boolean flush(final int flushLeastPages) {
		return this.mapedFileQueue.flush(flushLeastPages);
	}

	public void start() {
		this.flushIndexService.start();
		this.cleanIndexMappedFileService.start();
		if (this.defaultLogStorage.getStoreConfig().isTransientStorePoolEnable()) {
			this.commitIndexService.start();
		}
		varStorage.start();
	}

	public void shutdown() {
		if (this.defaultLogStorage.getStoreConfig().isTransientStorePoolEnable()) {
			this.commitIndexService.shutdown();
		}
		this.flushIndexService.shutdown();
		varStorage.shutdown();
	}

	@Override
	public boolean init() {
		boolean varLoadResult = true;
		if (this.varStorage == null) {
			this.varStorage = new VarStorage(storePath, groupId);
			varLoadResult = this.varStorage.load();
		}
		boolean result = this.mapedFileQueue.load();
		log.info("init index db {} - {}" , this.groupId, (result ? "OK" : "Failed"));
		this.flushIndexService = new FlushIndexService(defaultLogStorage, groupId, this);
		this.cleanIndexMappedFileService = new CleanIndexMappedFileService(groupId, this);
		if (this.defaultLogStorage.getStoreConfig().isTransientStorePoolEnable()) {
			this.commitIndexService = new CommitIndexService(defaultLogStorage, groupId, this);
		}
		return result && varLoadResult;
	}

	/**
	 * Just load the last file into memory
	 */
	public void recover() {
		final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
		if (!mapedFiles.isEmpty()) {
			// Recover from the third file from the bottom
			int index = mapedFiles.size() - 2;
			if (index < 0) {
				index = 0;
			}

			int mapedFileSizeLogics = this.mapedFileSize;
			for (int idx = index; idx < mapedFiles.size(); idx++) {
				MapedFile mapedFile = mapedFiles.get(idx);
				try {
					mapedFile.createMappedFile();
				} catch (IOException e) {
					e.printStackTrace();
					log.error("recover mappedfile failed, file : {}.", mapedFile.getFileName());
				}
			}

			MapedFile mapedFile = mapedFiles.get(index);
			ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
			long processOffset = mapedFile.getFileFromOffset();
			long mapedFileOffset = 0;
			while (true) {
				for (int i = 0; i < mapedFileSizeLogics; i += CQStoreUnitSize) {
					long offset = byteBuffer.getLong();
					byteBuffer.getInt();
					int size = byteBuffer.getInt();

					// Explain that the current storage unit is valid
					// TODO Is it reasonable to judge that this is effective?
					if (offset >= 0 && size > 0) {
						mapedFileOffset = i + CQStoreUnitSize;
					} else {
						log.info("recover current indexdb file over, {} {} {}.", mapedFile.getFileName(), offset, size);
						break;
					}
				}

				// Go to the end of the file, switch to the next file
				if (mapedFileOffset == mapedFileSizeLogics) {
					index++;
					if (index >= mapedFiles.size()) {
						// The current conditional branch cannot happen
						log.info("recover last indexdb file over, last maped file {}.", mapedFile.getFileName());
						break;
					} else {
						mapedFile = mapedFiles.get(index);
						byteBuffer = mapedFile.sliceByteBuffer();
						processOffset = mapedFile.getFileFromOffset();
						mapedFileOffset = 0;
						log.info("recover next indexdb file, {}.", mapedFile.getFileName());
					}
				} else {
					log.info("recover current indexdb over {} {}.", mapedFile.getFileName(), (processOffset + mapedFileOffset));
					break;
				}
			}

			processOffset += mapedFileOffset;
			this.mapedFileQueue.setFlushedWhere(processOffset);
			this.mapedFileQueue.setCommittedWhere(processOffset);
			log.info("indexdb commit offset : {}.", processOffset);

			correctMinInstanceID();
		}
	}

	@Override
	public void destroy() {
		this.minInstanceID = -1;
		this.mapedFileQueue.destroy();
		varStorage.destroy();
	}

	@Override
	public void deleteExpire(WriteOptions writeOptions, long maxInstanceId) {
		FileID index = getIndex(maxInstanceId);
		if (index != null) {
			deleteExpiredFile(index.getOffset());
		}
	}

	/**
	 * Correct the minimum instanceID at the beginning of the file
	 */
	private void correctMinInstanceID() {
		MapedFile firstMapFile = this.mapedFileQueue.getFirstMapedFileOnLock();
		if (firstMapFile != null) {
			this.minInstanceID = firstMapFile.getFileFromOffset() / CQStoreUnitSize;
		}
	}

	@Override
	public void correctMaxInstanceID(long maxInstanceID) {
		truncateDirtyIndexFiles(maxInstanceID);
	}

	private void truncateDirtyIndexFiles(long maxInstanceID) {
		// Size of each index file
		int indexFileSize = this.mapedFileSize;
		while (true) {
			MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile2();
			if (mapedFile != null) {
				ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
				// Clear Offset first
				mapedFile.setWrotePostion(0);
				mapedFile.setCommittedPosition(0);
				mapedFile.setFlushedPosition(0);

				boolean needContinue = true;

				for (int i = 0; i < indexFileSize; i += CQStoreUnitSize) {
					byteBuffer.getLong();
					byteBuffer.getInt();
					byteBuffer.getInt();

					long instanceID = (mapedFile.getFileFromOffset() + i) / CQStoreUnitSize;
					// Logical file start unit
					if (0 == i) {
						if (instanceID > maxInstanceID) {
							this.mapedFileQueue.deleteLastMapedFile();
							break;
						} else {
							int pos = i + CQStoreUnitSize;
							mapedFile.setWrotePostion(pos);
							mapedFile.setCommittedPosition(pos);
						}
					}
					// Logical file intermediate unit
					else {
						if (instanceID <= maxInstanceID) {
							int pos = i + CQStoreUnitSize;
							mapedFile.setWrotePostion(pos);
							mapedFile.setCommittedPosition(pos);

							if (pos == indexFileSize) {
								needContinue = false;
								break;
							}
						} else {
							needContinue = false;
							break;
						}
					}
				}
				if (!needContinue) {
					break;
				}
			} else {
				break;
			}
		}
	}

	/**
	 * Add index
	 *
	 * @param instanceID
	 * @param fileID
	 * @return
	 */
	@Override
	public boolean putIndex(WriteOptions writeOptions, long instanceID, FileID fileID) {
		this.putToIndexCache(instanceID, fileID);

		final long expectLogicOffset = instanceID * CQStoreUnitSize;

		MapedFile mapedFile = null;
		if (expectLogicOffset < this.mapedFileQueue.getMaxOffset()) {
			mapedFile = this.mapedFileQueue.findMapedFileByOffset(expectLogicOffset);
		} else {
			mapedFile = this.mapedFileQueue.getLastMapedFile(expectLogicOffset);
		}

		if (mapedFile != null) {
			try {
				ByteBuffer byteBuffer = ByteBuffer.allocate(CQStoreUnitSize);
				byteBuffer.putLong(fileID.getOffset());
				byteBuffer.putInt(fileID.getCrc32());
				byteBuffer.putInt(fileID.getSize());

				boolean res = mapedFile.appendData((int) (expectLogicOffset % mapedFileSize), byteBuffer.array());
				if (writeOptions.isSync()) {
					this.mapedFileQueue.flush();
				}

				if (!res) {
					log.error("putIndex failed, instanceID {}, fileID {}.", instanceID, fileID);
				}

				if (instanceID > this.maxInstanceID) {
					this.maxInstanceID = instanceID;
				}
			} catch (Exception e) {
				log.error("FileIndexDB putIndex failed, instanceID " + instanceID + " , groupID " + this.groupId, e);
				return false;
			}
			return true;
		} else {
			log.error("FileIndexDB get mappedFile null, instanceID {}, fileID {}.", instanceID, fileID);
		}

		return false;
	}

	/**
	 * Get the FileID corresponding to an index
	 *
	 * @param instanceID
	 * @return
	 */
	@Override
	public FileID getIndex(long instanceID) {
		if (instanceID < this.minInstanceID) {
			// File has been deleted
			return null;
		}

		FileID fileID = this.getFromCache(instanceID);
		if (fileID != null) {
			return fileID;
		}

		long offset = instanceID * CQStoreUnitSize;
		MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset);
		if (mapedFile != null) {
			try {

				fileID = mapedFile.selectFileID((int) (offset % mapedFileSize), CQStoreUnitSize);
				if (fileID == null || fileID.getOffset() < 0) {
					log.debug("getIndex failed, instanceID {}, groupID {}.", instanceID, this.groupId);
					return null;
				}

				return fileID;
			} catch (Exception e) {
				log.error("getIndex failed, instanceID " + instanceID, e);
			}
		}
		return null;
	}

	/**
	 * Delete an index
	 *
	 * @param instanceID
	 */
	@Override
	public void deleteOneIndex(long instanceID) {
		log.info("deleteOneIndex instanceID {}", instanceID);
		this.removeIndexCache(instanceID);
		if (instanceID < this.minInstanceID) {
			// File has been deleted
			return;
		}

		long offset = instanceID * CQStoreUnitSize;
		MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset);
		if (mapedFile != null) {
			log.info("deleteOneIndex mapedfile fromoffset : {}.", mapedFile.getFileFromOffset());
			try {
				FileID blankFileID = new FileID(-1, 0, 0);
				putIndex(new WriteOptions(false), instanceID, blankFileID);
			} catch (Exception e) {
				log.error("deleteIndex failed, instanceID " + instanceID, e);
			}
		}
	}

	private void putToIndexCache(long instanceID, FileID fileID) {
		this.indexMap.put(instanceID, fileID);
	}

	private FileID getFromCache(long instanceID) {
		return this.indexMap.get(instanceID);
	}

	private void removeIndexCache(long instanceID) {
		this.indexMap.remove(instanceID);
	}

	public void clearIndexCache() {
		if (this.indexMap.size() > maxIndexCache) {
			this.indexMap.clear();
		}
	}

	/**
	 * Delete files in bulk
	 *
	 * @param offset
	 * @return
	 */
	public int deleteExpiredFile(long offset) {
		int cnt = this.mapedFileQueue.deleteExpiredIndexFileByOffset(offset, CQStoreUnitSize);
		// Regardless of whether the file is deleted, the minimum value needs to be corrected, because it is possible that the physical file has been deleted.
		// But none of the logical files can be deleted
		this.correctMinInstanceID();
		return cnt;
	}

	public String getStorePath() {
		return storePath;
	}

	public MapedFileQueue getMapedFileQueue() {
		return mapedFileQueue;
	}

	public FlushIndexService getFlushIndexService() {
		return flushIndexService;
	}

	@Override
	public byte[] getMaxInstanceID() {
		return varStorage.getMaxInstanceID();
	}

	@Override
	public void setMaxInstanceID(WriteOptions writeOptions, long instanceId) {
		varStorage.setMaxInstanceID(maxInstanceID);
		if (writeOptions.isSync()) {
			varStorage.persist();
		}
	}

	@Override
	public void setMastervariables(WriteOptions writeOptions, byte[] buffer) {
		varStorage.setMastervariables(buffer);
		if (writeOptions.isSync()) {
			varStorage.persist();
		}
	}

	@Override
	public byte[] getMastervariables() {
		return varStorage.getMastervariables();
	}

	@Override
	public byte[] getSystemvariables() {
		return varStorage.getSystemvariables();
	}

	@Override
	public void setSystemvariables(WriteOptions writeOptions, byte[] buffer) {
		varStorage.setSystemvariables(buffer);
		if (writeOptions.isSync()) {
			varStorage.persist();
		}
	}

	@Override
	public void setMinChosenInstanceID(WriteOptions writeOptions, long instanceId) {
		varStorage.setMinChosenInstanceID(instanceId);
		if (writeOptions.isSync()) {
			varStorage.persist();
		}
	}

	@Override
	public byte[] getMinChosenInstanceID() {
		return varStorage.getMinChosenInstanceID();
	}
}
