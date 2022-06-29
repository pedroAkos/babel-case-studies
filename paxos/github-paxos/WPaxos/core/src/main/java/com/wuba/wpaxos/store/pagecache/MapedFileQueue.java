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
package com.wuba.wpaxos.store.pagecache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.store.GetResult;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.store.service.AllocateMapedFileService;
import com.wuba.wpaxos.utils.UtilAll;

/**
 * Mapedfile file storage queue, unlimited growth
 */
public class MapedFileQueue {
	private static final Logger log = LogManager.getLogger(MapedFileQueue.class);
	// File storage location
	private final String storePath;
	// The size of each file
	private final int mapedFileSize;
	// Individual files
	private final List<MapedFile> mapedFiles = new ArrayList<MapedFile>();
	// Read-write lock (for mapedFiles)
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	// Pre-allocated MapedFile object service
	private final AllocateMapedFileService allocateMapedFileService;
	// Where is the brush?
	private long committedWhere = 0;
	private long flushedWhere = 0;
	// Last message storage time
	private volatile long storeTimestamp = 0;

	public MapedFileQueue(final String storePath, int mapedFileSize, AllocateMapedFileService allocateMapedFileService) {
		this.storePath = storePath;
		this.mapedFileSize = mapedFileSize;
		this.allocateMapedFileService = allocateMapedFileService;
	}

	public void checkSelf() {
		this.readWriteLock.readLock().lock();
		try {
			if (!this.mapedFiles.isEmpty()) {
				MapedFile first = this.mapedFiles.get(0);
				MapedFile last = this.mapedFiles.get(this.mapedFiles.size() - 1);

				int sizeCompute =
						(int) ((last.getFileFromOffset() - first.getFileFromOffset()) / this.mapedFileSize) + 1;
				int sizeReal = this.mapedFiles.size();
				if (sizeCompute != sizeReal) {
					log.error(
							"[BUG]The mapedfile queue's data is damaged, {} mapedFileSize={} sizeCompute={} sizeReal={}\n{}", //
							this.storePath,//
							this.mapedFileSize,//
							sizeCompute,//
							sizeReal,//
							this.mapedFiles.toString()//
					);
				}
			}
		} finally {
			this.readWriteLock.readLock().unlock();
		}
	}

	public MapedFile getMapedFileByTime(final long timestamp) {
		Object[] mfs = this.copyMapedFiles(0);

		if (null == mfs) {
			return null;
		}

		for (int i = 0; i < mfs.length; i++) {
			MapedFile mapedFile = (MapedFile) mfs[i];
			if (mapedFile.getLastModifiedTimestamp() >= timestamp) {
				return mapedFile;
			}
		}

		return (MapedFile) mfs[mfs.length - 1];
	}

	private Object[] copyMapedFiles(final int reservedMapedFiles) {
		Object[] mfs = null;

		try {
			this.readWriteLock.readLock().lock();
			if (this.mapedFiles.size() <= reservedMapedFiles) {
				return null;
			}

			mfs = this.mapedFiles.toArray();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.readWriteLock.readLock().unlock();
		}
		return mfs;
	}

	/**
	 * Called when recover, no need to lock
	 */
	public void truncateDirtyFiles(long offset) {
		List<MapedFile> willRemoveFiles = new ArrayList<MapedFile>();

		for (MapedFile file : this.mapedFiles) {
			long fileTailOffset = file.getFileFromOffset() + this.mapedFileSize;
			if (fileTailOffset > offset) {
				if (offset >= file.getFileFromOffset()) {
					file.setWrotePostion((int) (offset % this.mapedFileSize));
					file.setCommittedPosition((int) (offset % this.mapedFileSize));
					file.setFlushedPosition((int) (offset % this.mapedFileSize));
				} else {
					// Delete the file
					file.destroy(1000);
					willRemoveFiles.add(file);
				}
			}
		}

		this.deleteExpiredFile(willRemoveFiles);
	}

	/**
	 * Delete files can only be deleted from the beginning
	 */
	private void deleteExpiredFile(List<MapedFile> files) {
		if (!files.isEmpty()) {
			try {
				this.readWriteLock.writeLock().lock();
				for (MapedFile file : files) {
					if (!this.mapedFiles.remove(file)) {
						log.error("deleteExpiredFile remove failed.");
						break;
					}
					log.info("delete expire file success, file name : {}.", file.getFileName());
				}
			} catch (Exception e) {
				log.error("deleteExpiredFile has exception.", e);
			} finally {
				this.readWriteLock.writeLock().unlock();
			}
		}
	}

	public boolean load() {
		File dir = new File(this.storePath);
		File[] files = dir.listFiles();
		if (files != null) {
			// ascending order
			Arrays.sort(files);
			for (File file : files) {
				// Verify that the file size matches
				if (file.length() != this.mapedFileSize) {
					log.warn(file + "\t" + file.length() + " length not matched message store config value, ignore it");
					return true;
				}

				// Recovery queue
				try {
					MapedFile mapedFile = new MapedFile(file.getPath(), mapedFileSize, false);
					mapedFile.setWrotePostion(this.mapedFileSize);
					mapedFile.setCommittedPosition(this.mapedFileSize);
					mapedFile.setFlushedPosition(this.mapedFileSize);
					this.mapedFiles.add(mapedFile);
					log.info("init " + file.getPath() + " OK");
				} catch (IOException e) {
					log.error("init file " + file + " error", e);
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * How much is behind the progress
	 */
	public long howMuchFallBehind() {
		if (this.mapedFiles.isEmpty()) {
			return 0;
		}

		long committed = this.flushedWhere;
		if (committed != 0) {
			MapedFile mapedFile = this.getLastMapedFile();
			if (mapedFile != null) {
				return (mapedFile.getFileFromOffset() + mapedFile.getWrotePostion()) - committed;
			}
		}
		return 0;
	}

	public MapedFile getLastMapedFile() {
		return this.getLastMapedFile(0);
	}

	/**
	 * Get the last MapedFile object, if there is none, create a new one, if the last one is full, create a new one
	 *
	 * @param startOffset If you create a new file, start offset
	 * @return
	 */
	public MapedFile getLastMapedFile(final long startOffset) {
		long createOffset = -1;
		MapedFile mapedFileLast = null;

		try {
			this.readWriteLock.readLock().lock();
			if (this.mapedFiles.isEmpty()) {
				createOffset = startOffset - (startOffset % this.mapedFileSize);
			} else {
				mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
			}
		} finally {
			this.readWriteLock.readLock().unlock();
		}

		if (mapedFileLast != null && mapedFileLast.isFull()) {
			createOffset = mapedFileLast.getFileFromOffset() + this.mapedFileSize;
		}

		if (createOffset != -1) {
			String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
			String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mapedFileSize);
			MapedFile mapedFile = null;

			if (this.allocateMapedFileService != null) {
				mapedFile = this.allocateMapedFileService.putRequestAndReturnMapedFile(nextFilePath, nextNextFilePath, this.mapedFileSize);
			} else {
				try {
					mapedFile = new MapedFile(nextFilePath, this.mapedFileSize, true);
				} catch (IOException e) {
					log.error("create mapedfile exception", e);
				}
			}

			if (mapedFile != null) {
				try {
					this.readWriteLock.writeLock().lock();
					if (this.mapedFiles.isEmpty()) {
						mapedFile.setFirstCreateInQueue(true);
					}
					this.mapedFiles.add(mapedFile);
					log.info("MapedFileQueue add mapedFile {}.", mapedFile.getFileName());
				} finally {
					this.readWriteLock.writeLock().unlock();
				}
			}
			return mapedFile;
		}
		return mapedFileLast;
	}

	/**
	 * Get the minimum Offset of the queue, if the queue is empty, return -1
	 */
	public long getMinOffset() {
		try {
			this.readWriteLock.readLock().lock();
			if (!this.mapedFiles.isEmpty()) {
				return this.mapedFiles.get(0).getFileFromOffset();
			}
		} catch (Exception e) {
			log.error("getMinOffset has exception.", e);
		} finally {
			this.readWriteLock.readLock().unlock();
		}

		return -1;
	}

	public long getMaxOffset() {
		try {
			this.readWriteLock.readLock().lock();
			if (!this.mapedFiles.isEmpty()) {
				int lastIndex = this.mapedFiles.size() - 1;
				MapedFile mapedFile = this.mapedFiles.get(lastIndex);
				return mapedFile.getFileFromOffset() + mapedFile.getWrotePostion();
			}
		} catch (Exception e) {
			log.error("getMaxOffset has exception.", e);
		} finally {
			this.readWriteLock.readLock().unlock();
		}

		return 0;
	}

	/**
	 * Called on resume
	 */
	public void deleteLastMapedFile() {
		if (!this.mapedFiles.isEmpty()) {
			int lastIndex = this.mapedFiles.size() - 1;
			MapedFile mapedFile = this.mapedFiles.get(lastIndex);
			mapedFile.destroy(1000);
			this.mapedFiles.remove(mapedFile);
			log.info("on recover, destroy a logic maped file " + mapedFile.getFileName());
		}
	}

	/**
	 * Delete physical queue files according to file expiration time
	 */
	public int deleteExpiredFileByTime(
			final long expiredTime,
			final int deleteFilesInterval,
			final long intervalForcibly,
			final boolean cleanImmediately) {
		Object[] mfs = this.copyMapedFiles(0);

		if (null == mfs)
			return 0;

		// The last file is in the write state and cannot be deleted
		int mfsLength = mfs.length - 1;
		int deleteCount = 0;
		List<MapedFile> files = new ArrayList<MapedFile>();
		for (int i = 0; i < mfsLength; i++) {
			MapedFile mapedFile = (MapedFile) mfs[i];
			long liveMaxTimestamp = mapedFile.getLastModifiedTimestamp() + expiredTime;
			if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
				if (mapedFile.destroy(intervalForcibly)) {
					files.add(mapedFile);
					deleteCount++;

					int deleteFilesBatchMax = StoreConfig.deleteFilesBatchMax;
					if (files.size() >= deleteFilesBatchMax) {
						break;
					}

					if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
						try {
							Thread.sleep(deleteFilesInterval);
						} catch (InterruptedException e) {
						}
					}
				} else {
					log.error("delete expired file by time failed : {}, notify admin.", mapedFile.getFileName());
					break;
				}
			}
		}
		deleteExpiredFile(files);
		return deleteCount;
	}

	/**
	 * Delete the logical queue according to the minimum Offset of the physical queue
	 *
	 * @param offset Minimum offset of physical queue
	 */
	public int deleteExpiredFileByOffset(long offset, int unitSize) {
		Object[] mfs = this.copyMapedFiles(0);

		List<MapedFile> files = new ArrayList<MapedFile>();
		int deleteCount = 0;
		if (null != mfs) {
			// The last file is in the write state and cannot be deleted
			int mfsLength = mfs.length - 1;

			// Traverse range here 0 ... last - 1
			for (int i = 0; i < mfsLength; i++) {
				boolean destroy = true;
				MapedFile mapedFile = (MapedFile) mfs[i];
				GetResult result = mapedFile.selectMapedBuffer(this.mapedFileSize - unitSize);
				if (result != null) {
					result.getByteBuffer().getLong();
					long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
					result.release();
					// Can the current file be deleted
					destroy = (maxOffsetInLogicQueue < offset);
					if (destroy) {
						log.info("physic min offset {}, logics in current mapedfile max offset {}, delete it", offset, maxOffsetInLogicQueue);
					}
				} else {
					log.warn("this being not excuted forever.");
					break;
				}

				if (destroy && mapedFile.destroy(1000 * 60)) {
					files.add(mapedFile);
					deleteCount++;
				} else {
					break;
				}
			}
		}
		deleteExpiredFile(files);
		return deleteCount;
	}


	public int deleteExpiredPhyFileByOffset(long offset, int fileSize) {
		Object[] mfs = this.copyMapedFiles(0);

		List<MapedFile> files = new ArrayList<MapedFile>();
		int deleteCount = 0;
		if (null != mfs) {
			// The last file is in the write state and cannot be deleted
			int mfsLength = mfs.length - 1;

			// Traverse range here 0 ... last - 1
			for (int i = 0; i < mfsLength; i++) {
				boolean destroy = false;
				MapedFile mapedFile = (MapedFile) mfs[i];
				long fileOffset = mapedFile.getFileFromOffset() + fileSize;
				if (fileOffset < offset) {
					destroy = true;
					log.info("physic min offset {}, logics in current mapedfile max offset {}, delete it", offset, fileOffset);
				}
				if (destroy && mapedFile.destroy(1000 * 60)) {
					files.add(mapedFile);
					deleteCount++;
				} else {
					break;
				}
			}
		}
		deleteExpiredFile(files);
		return deleteCount;
	}


	/**
	 * Delete the index file according to the minimum Offset of the physical queue
	 *
	 * @param offset Minimum offset of physical queue
	 */
	public int deleteExpiredIndexFileByOffset(long offset, int unitSize) {
		Object[] mfs = this.copyMapedFiles(0);

		List<MapedFile> files = new ArrayList<MapedFile>();
		int deleteCount = 0;
		if (null != mfs) {
			// The last file is in the write state and cannot be deleted
			int mfsLength = mfs.length - 1;

			// Traverse range here 0 ... last - 1
			for (int i = 0; i < mfsLength; i++) {
				boolean destroy = true;
				MapedFile mapedFile = (MapedFile) mfs[i];
				GetResult result = mapedFile.selectMapedBuffer(this.mapedFileSize - unitSize);
				if (result != null) {
					long maxOffset = result.getByteBuffer().getLong();
					if (maxOffset == -1) {
						break;
					}
					result.release();
					// Can the current file be deleted
					destroy = (maxOffset < offset);
					if (destroy) {
						log.info("physic min offset {}, index file current max offset {}, delete it", offset, maxOffset);
					}
				} else {
					log.info("deleteExpiredIndexFileByOffset, mapedFile fromoffset {}, writepostion {}, pos {}.", mapedFile.getFileFromOffset(), mapedFile.getWrotePostion(), (this.mapedFileSize - unitSize));
					log.warn("this being not excuted forever.");
					break;
				}

				if (destroy && mapedFile.destroy(1000 * 60)) {
					files.add(mapedFile);
					deleteCount++;
				} else {
					break;
				}
			}
		}
		deleteExpiredFile(files);
		return deleteCount;
	}

	/**
	 * Delete the logical Error queue according to the minimum Offset of the physical queue
	 *
	 * @param offset Minimum offset of physical queue
	 */
	public int deleteExpiredErrorFileByOffset(long offset, int unitSize) {
		Object[] mfs = this.copyMapedFiles(0);

		List<MapedFile> files = new ArrayList<MapedFile>();
		int deleteCount = 0;
		if (null != mfs) {
			// The last file is in the write state and cannot be deleted
			int mfsLength = mfs.length - 1;

			// Traverse range here 0 ... last - 1
			for (int i = 0; i < mfsLength; i++) {
				boolean destroy = true;
				MapedFile mapedFile = (MapedFile) mfs[i];
				long maxOffsetInLogicQueue = 0;
				GetResult result = mapedFile.selectMapedBuffer(0);
				if (result != null) {
					try {
						// News exists
						for (int j = 0; j < result.getSize(); j += unitSize) {
							long offsetPy = result.getByteBuffer().getLong();
							result.getByteBuffer().getInt();
							if (offsetPy > maxOffsetInLogicQueue) {
								maxOffsetInLogicQueue = offsetPy;
							}
						}
					} catch (Exception ex) {
						log.error("", ex);
					} finally {
						result.release();
					}
				} else {
					log.warn("this being not excuted forever.");
					break;
				}

				destroy = (maxOffsetInLogicQueue < offset);

				if (destroy && mapedFile.destroy(1000 * 60)) {
					files.add(mapedFile);
					deleteCount++;
				} else {
					break;
				}
			}
		}
		deleteExpiredFile(files);
		return deleteCount;
	}

	public boolean flush() {
		boolean result = true;
		result &= this.commit(0);
		result &= this.flush(0);
		return result;
	}

	/**
	 * The return value indicates whether all flashing is completed
	 *
	 * @return
	 */
	public boolean flush(final int flushLeastPages) {
		boolean result = true;
		MapedFile mappedFile = this.findMapedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
		if (mappedFile != null) {
			long tmpTimeStamp = mappedFile.getStoreTimestamp();
			int offset = mappedFile.flush(flushLeastPages);
			long where = mappedFile.getFileFromOffset() + offset;
			result = where == this.flushedWhere;
			this.flushedWhere = where;
			if (0 == flushLeastPages) {
				this.storeTimestamp = tmpTimeStamp;
			}
		}

		return result;
	}

	public boolean commit(final int commitLeastPages) {
		boolean result = true;
		MapedFile mappedFile = this.findMapedFileByOffset(this.committedWhere, this.committedWhere == 0);
		if (mappedFile != null) {
			int offset = mappedFile.commit(commitLeastPages);
			long where = mappedFile.getFileFromOffset() + offset;
			result = where == this.committedWhere;
			this.committedWhere = where;
		}

		return result;
	}

	public MapedFile findMapedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
		try {
			this.readWriteLock.readLock().lock();
			MapedFile mapedFile = this.getFirstMapedFile();

			if (mapedFile != null) {
				int index = (int) ((offset / this.mapedFileSize) - (mapedFile.getFileFromOffset() / this.mapedFileSize));
				if (index < 0 || index >= this.mapedFiles.size()) {
					log.warn(
							"findMapedFileByOffset offset not matched, request Offset: {}, "
									+ "index: {}, mapedFileSize: {}, mapedFiles count: {}, "
									+ "getFileFromOffset: {}, StackTrace: {}",//
							offset,//
							index,//
							this.mapedFileSize,//
							this.mapedFiles.size(),//
							mapedFile.getFileFromOffset(),
							UtilAll.currentStackTrace());
				}

				try {
					return this.mapedFiles.get(index);
				} catch (Exception e) {
					if (returnFirstOnNotFound) {
						return mapedFile;
					}
				}
			}
		} catch (Exception e) {
			log.error("findMapedFileByOffset Exception", e);
		} finally {
			this.readWriteLock.readLock().unlock();
		}

		return null;
	}

	public void cleanUnMappedFile() {
		Object[] mfs = this.copyMapedFiles(0);

		if (mfs != null) {
			for (int i = 0; i < mfs.length - 1; i++) {
				MapedFile mapedFile = (MapedFile) mfs[i];
				try {
					if (mapedFile.getIsMapped() && mapedFile.unTouchCheck() && (mapedFile.getRefCount() <= 1)) {
						mapedFile.unMapFile();
					}
				} catch (IOException e) {
					log.error("unmap file {} failed.", mapedFile.getFileName());
				}
			}
		}
	}

	private MapedFile getFirstMapedFile() {
		if (this.mapedFiles.isEmpty()) {
			return null;
		}

		return this.mapedFiles.get(0);
	}

	public MapedFile getLastMapedFile2() {
		if (this.mapedFiles.isEmpty()) {
			return null;
		}
		return this.mapedFiles.get(this.mapedFiles.size() - 1);
	}

	public MapedFile findMapedFileByOffset(final long offset) {
		return findMapedFileByOffset(offset, false);
	}

	public long getMapedMemorySize() {
		long size = 0;

		Object[] mfs = this.copyMapedFiles(0);
		if (mfs != null) {
			for (Object mf : mfs) {
				if (((ReferenceResource) mf).isAvailable()) {
					size += this.mapedFileSize;
				}
			}
		}

		return size;
	}

	public boolean retryDeleteFirstFile(final long intervalForcibly) {
		MapedFile mapedFile = this.getFirstMapedFileOnLock();
		if (mapedFile != null) {
			if (!mapedFile.isAvailable()) {
				log.warn("the mapedfile was destroyed once, but still alive, {}.", mapedFile.getFileName());
				boolean result = mapedFile.destroy(intervalForcibly);
				if (result) {
					log.warn("the mapedfile redelete OK, {}.", mapedFile.getFileName());
					List<MapedFile> tmps = new ArrayList<MapedFile>();
					tmps.add(mapedFile);
					this.deleteExpiredFile(tmps);
				} else {
					log.warn("the mapedfile redelete Failed, {}.", mapedFile.getFileName());
				}

				return result;
			}
		}

		return false;
	}

	public MapedFile getFirstMapedFileOnLock() {
		try {
			this.readWriteLock.readLock().lock();
			return this.getFirstMapedFile();
		} finally {
			this.readWriteLock.readLock().unlock();
		}
	}

	/**
	 * Close the queue, the queue data is still there, but it cannot be accessed
	 */
	public void shutdown(final long intervalForcibly) {
		try {
			this.readWriteLock.readLock().lock();
			for (MapedFile mf : this.mapedFiles) {
				mf.shutdown(intervalForcibly);
			}
		} finally {
			this.readWriteLock.readLock().unlock();
		}
	}

	/**
	 * Destroy the queue, the queue data is deleted, this function may be unsuccessful
	 */
	public void destroy() {
		try {
			this.readWriteLock.writeLock().lock();
			if (!mapedFiles.isEmpty()) {
				long lastFileOffset = this.mapedFiles.get(this.mapedFiles.size() - 1).getFileFromOffset();
				String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(lastFileOffset + this.mapedFileSize);
				if (allocateMapedFileService != null) {
					allocateMapedFileService.removeFile(nextFilePath);
				}
			}

			for (MapedFile mf : this.mapedFiles) {
				mf.destroy(1000 * 3);
			}
			this.mapedFiles.clear();
			this.flushedWhere = 0;

			// delete parent directory
			File file = new File(storePath);
			if (file.isDirectory()) {
				file.delete();
			}
		} finally {
			this.readWriteLock.writeLock().unlock();
		}
	}

	public long getCommittedWhere() {
		return committedWhere;
	}

	public void setCommittedWhere(long committedWhere) {
		this.committedWhere = committedWhere;
	}

	public long getFlushedWhere() {
		return flushedWhere;
	}

	public void setFlushedWhere(long flushedWhere) {
		this.flushedWhere = flushedWhere;
	}

	public long getStoreTimestamp() {
		return storeTimestamp;
	}

	public List<MapedFile> getMapedFiles() {
		return mapedFiles;
	}

	public int getMapedFileSize() {
		return mapedFileSize;
	}
}
