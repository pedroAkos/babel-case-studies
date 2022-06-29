/*
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
package com.wuba.wpaxos.store.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.store.db.FileIndexDB;

/**
 * paxos log storage config
 */
public class StoreConfig implements DynamicConfig{

	private static final Logger logger = LogManager.getLogger(StoreConfig.class);

	private final String storeRootPath;

	private String storeConfigPath;

	// CommitLog each file size
	private int mapedFileSizePhysic= 1024 * 1024 * 100;
//	private int mapedFileSizePhysic= 1024 * 1024 * 1;

	private int maxIndexNum = 300000;
	// indexDB each file size
	private int mapedFileSizeIndexDB = 300000 * FileIndexDB.CQStoreUnitSize;

	// PhysicLog flashing interval (unit: milliseconds)
	private int flushIntervalPhysicLog = 1000;

	// Whether to refresh the disk regularly, the default is to refresh the disk in real time
	private boolean flushPhysicTimed = false;

	// IndexDB flushing interval (in milliseconds)
	private int flushIntervalIndexdb = 1000;

    // commit data to FileChannel
    private int commitIntervalPhysicLog = 200;
    // commit data to FileChannel
    private int commitIntervalIndexdb = 200;

	// StateMachine Checkpoint flashing interval (in milliseconds)
	private int flushIntervalStateMachineCheckpoint = 1000;

	// Time between resource cleaning (in milliseconds)
	private int cleanResourceInterval = 60000;

	// Time to detect mappedfile that does not require memory mapping (in milliseconds)
	private int checkUnmapfileInterval = 60000;

	// The interval between deleting multiple MessageLog files (in milliseconds)
	private int deletePhysicLogFilesInterval = 100;

	// Delete indexDB file interval
//	private int deleteIndexFilesInterval = 100;
	private int deleteIndexIdInterval = 10000;

	// Interval time for forcibly deleting files (in milliseconds)
	private int destroyMapedFileIntervalForcibly = 1000 * 120;

	public static int deleteFilesBatchMax = 2;

	private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;

	// Synchronous flashing timeout
	private int syncFlushTimeout = 1000;

	// When to trigger file deletion, delete hour
	private String deleteTime = "04";

	// File retention time (in hours)
	private int fileReservedTime = 72;

	// Maximum disk space usage
	private int diskMaxUsedSpaceRatio = 75;

	// Disk space warning water level, if exceeded, stop receiving new messages (for self-protection purposes)
	private double diskSpaceWarningLevelRatio = 0.90;

	// Disk space forcibly delete file water mark
	private double diskSpaceCleanForciblyRatio = 0.85;

	// Write message index to ConsumeQueue, buffer high water level, flow control starts when it exceeds
	private int putMsgIndexHightWater = 600000;

	// Maximum message size, 512K by default * 20
	private int maxMessageSize = 1024 * 512 * 20;

	// When restarting, whether to check the CRC
	private boolean checkCRCOnRecover = true;

	// Brush MessageLog, at least a few pages
	private int flushPhysicLogLeastPages = 4;

    // How many pages are to be committed when commit data to file
    private int commitPhysicLogLeastPages = 4;

    private int commitIndexDBLeastPages = 2;

	// Brush IndexFile, at least brush a few PAGE
	private int flushIndexDBLeastPages = 1;

	// Brush MessageLog, the interval time of thoroughly brushing the disk
	private int flushPhysicLogThoroughInterval = 1000 * 10;

	private int commitPhysicLogThoroughInterval = 200;
	//private int commitCommitLogThoroughInterval = 10;

	private int commitIndexToroughInterval = 200;
	//private int commitIndexToroughInterval = 10;

	private int flushIndexThoroughInterval = 1000 * 10;

	// Periodically check Hanged file interval (in milliseconds)
	private int redeleteHangedFileInterval = 1000 * 120;

	// The maximum number of message bytes to be pulled, the message is in the memory
	private int maxTransferBytesOnMessageInMemory = 1024 * 512; // 1024 * 512 * 100

	// The maximum number of messages to be pulled, the messages are in the memory
	private int maxTransferCountOnMessageInMemory = 128;

	// The maximum number of message bytes to be pulled, the message is on the disk
	private int maxTransferBytesOnMessageInDisk = 1024 * 512; // 1024 * 512 * 100

	// The maximum number of messages to be pulled, the messages are on disk
	private int maxTransferCountOnMessageInDisk = 64;

	// Maximum percentage of hit messages in memory
	private int accessMessageInMemoryMaxRatio = 40;

	// If the disk space exceeds the 90% warning level, the file will be deleted automatically
	private boolean cleanFileForciblyEnable = true;

	private int storeCheckPointFlushInterval = 30000;

	private boolean warmMapedFileEnable = false;
    // Flush page size when the disk in warming state
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;

    private boolean transientStorePoolEnable = false;

    private int transientStorePoolSize = 20;

    private int transientStoreIndexDBPoolSize = 20;

    private boolean fastFailIfNoBufferInStorePool = false;

    private boolean isInit = false;

	// The longest time that the mapedfile has not been accessed in the mapped memory
	public static int maxMapedfileUntouchTime = 1000 * 60 * 3;

	public StoreConfig(String storeRootPath, String storeConfigPath) {
		if (storeRootPath == null) {
			storeRootPath = System.getProperty("usr.dir") + File.separator + "db";
			logger.info("not init storeRootPath, use default path : {}.", storeRootPath);
		}

		this.storeRootPath = storeRootPath;
		this.storeConfigPath = storeConfigPath;
		if (this.storeConfigPath != null) {
			try {
				loadConfig();
			} catch (Exception e) {
				logger.error("init store config error", e);
			}
			initConfigLoad();
		}
	}

	public void initConfigLoad() {
		if (isInit) {
			return;
		}

		try {
			StoreConfigLoader fileLoad = StoreConfigLoader.getInstance();
			fileLoad.setFileInfo(storeConfigPath);
			fileLoad.start(this);
		} catch(Exception e) {
			logger.error("StoreFactory init throws exception", e);
		}
		isInit = true;
	}

	/**
	 * Will only load once
	 * @throws Exception
	 */
	@Override
	public void loadConfig() throws Exception {
		FileInputStream fileInputStream = null;
		Properties properties = new Properties();
		try {
			fileInputStream = new FileInputStream(this.storeConfigPath);
			properties.load(fileInputStream);
			properties2Object(properties, this);
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch(IOException e) {
//					logger.warn("inputStream close IOException:" + e.getMessage());
				}
			}
		}
	}

	public int getMapedFileSizePhysic() {
		return mapedFileSizePhysic;
	}

	public void setMapedFileSizePhysic(int mapedFileSizePhysic) {
		this.mapedFileSizePhysic = mapedFileSizePhysic;
	}

	public int getSyncFlushTimeout() {
		return syncFlushTimeout;
	}

	public void setSyncFlushTimeout(int syncFlushTimeout) {
		this.syncFlushTimeout = syncFlushTimeout;
	}

	public int getMaxMessageSize() {
		return maxMessageSize;
	}

	public void setMaxMessageSize(int maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
	}

	public int getMapedFileSizeIndexDB() {
		return mapedFileSizeIndexDB;
	}

	public void setMapedFileSizeIndexDB(int mapedFileSizeIndexDB) {
		this.mapedFileSizeIndexDB = mapedFileSizeIndexDB;
	}

	public int getFlushIntervalPhysicLog() {
		return flushIntervalPhysicLog;
	}

	public void setFlushIntervalPhysicLog(int flushIntervalPhysicLog) {
		this.flushIntervalPhysicLog = flushIntervalPhysicLog;
	}

	public boolean isFlushPhysicTimed() {
		return flushPhysicTimed;
	}

	public void setFlushPhysicTimed(boolean flushPhysicTimed) {
		this.flushPhysicTimed = flushPhysicTimed;
	}

	public int getFlushIntervalIndexdb() {
		return flushIntervalIndexdb;
	}

	public void setFlushIntervalIndexdb(int flushIntervalIndexdb) {
		this.flushIntervalIndexdb = flushIntervalIndexdb;
	}

	public int getCommitIntervalIndexdb() {
		return commitIntervalIndexdb;
	}

	public void setCommitIntervalIndexdb(int commitIntervalIndexdb) {
		this.commitIntervalIndexdb = commitIntervalIndexdb;
	}

	public int getFlushIntervalStateMachineCheckpoint() {
		return flushIntervalStateMachineCheckpoint;
	}

	public void setFlushIntervalStateMachineCheckpoint(
			int flushIntervalStateMachineCheckpoint) {
		this.flushIntervalStateMachineCheckpoint = flushIntervalStateMachineCheckpoint;
	}

	public int getCleanResourceInterval() {
		return cleanResourceInterval;
	}

	public void setCleanResourceInterval(int cleanResourceInterval) {
		this.cleanResourceInterval = cleanResourceInterval;
	}

	public int getCheckUnmapfileInterval() {
		return checkUnmapfileInterval;
	}

	public void setCheckUnmapfileInterval(int checkUnmapfileInterval) {
		this.checkUnmapfileInterval = checkUnmapfileInterval;
	}

	public int getDeletePhysicLogFilesInterval() {
		return deletePhysicLogFilesInterval;
	}

	public void setDeletePhysicLogFilesInterval(int deletePhysicLogFilesInterval) {
		this.deletePhysicLogFilesInterval = deletePhysicLogFilesInterval;
	}

	public int getDeleteIndexIdInterval() {
		return deleteIndexIdInterval;
	}

	public void setDeleteIndexIdInterval(int deleteIndexIdInterval) {
		this.deleteIndexIdInterval = deleteIndexIdInterval;
	}

	public int getDestroyMapedFileIntervalForcibly() {
		return destroyMapedFileIntervalForcibly;
	}

	public void setDestroyMapedFileIntervalForcibly(
			int destroyMapedFileIntervalForcibly) {
		this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
	}

	public static int getDeleteFilesBatchMax() {
		return deleteFilesBatchMax;
	}

	public static void setDeleteFilesBatchMax(int deleteFilesBatchMax) {
		StoreConfig.deleteFilesBatchMax = deleteFilesBatchMax;
	}

	public FlushDiskType getFlushDiskType() {
		return flushDiskType;
	}

	public void setFlushDiskType(FlushDiskType flushDiskType) {
		this.flushDiskType = flushDiskType;
	}

	public String getDeleteTime() {
		return deleteTime;
	}

	public void setDeleteWhen(String deleteTime) {
		this.deleteTime = deleteTime;
	}

	public int getFileReservedTime() {
		return fileReservedTime;
	}

	public void setFileReservedTime(int fileReservedTime) {
		this.fileReservedTime = fileReservedTime;
	}

	public int getDiskMaxUsedSpaceRatio() {
		return diskMaxUsedSpaceRatio;
	}

	public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
		this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
	}

	public double getDiskSpaceWarningLevelRatio() {
		return diskSpaceWarningLevelRatio;
	}

	public void setDiskSpaceWarningLevelRatio(double diskSpaceWarningLevelRatio) {
		this.diskSpaceWarningLevelRatio = diskSpaceWarningLevelRatio;
	}

	public double getDiskSpaceCleanForciblyRatio() {
		return diskSpaceCleanForciblyRatio;
	}

	public void setDiskSpaceCleanForciblyRatio(double diskSpaceCleanForciblyRatio) {
		this.diskSpaceCleanForciblyRatio = diskSpaceCleanForciblyRatio;
	}

	public int getPutMsgIndexHightWater() {
		return putMsgIndexHightWater;
	}

	public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
		this.putMsgIndexHightWater = putMsgIndexHightWater;
	}

	public boolean isCheckCRCOnRecover() {
		return checkCRCOnRecover;
	}

	public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
		this.checkCRCOnRecover = checkCRCOnRecover;
	}

	public int getFlushPhysicLogLeastPages() {
		return flushPhysicLogLeastPages;
	}

	public void setFlushPhysicLogLeastPages(int flushPhysicLogLeastPages) {
		this.flushPhysicLogLeastPages = flushPhysicLogLeastPages;
	}

	public int getCommitPhysicLogLeastPages() {
		return commitPhysicLogLeastPages;
	}

	public void setCommitPhysicLogLeastPages(int commitPhysicLogLeastPages) {
		this.commitPhysicLogLeastPages = commitPhysicLogLeastPages;
	}

	public int getFlushPhysicLogThoroughInterval() {
		return flushPhysicLogThoroughInterval;
	}

	public void setFlushPhysicLogThoroughInterval(int flushPhysicLogThoroughInterval) {
		this.flushPhysicLogThoroughInterval = flushPhysicLogThoroughInterval;
	}

	public int getCommitPhysicLogThoroughInterval() {
		return commitPhysicLogThoroughInterval;
	}

	public void setCommitPhysicLogThoroughInterval(
			int commitPhysicLogThoroughInterval) {
		this.commitPhysicLogThoroughInterval = commitPhysicLogThoroughInterval;
	}

	public int getCommitIndexDBLeastPages() {
		return commitIndexDBLeastPages;
	}

	public void setCommitIndexDBLeastPages(int commitIndexDBLeastPages) {
		this.commitIndexDBLeastPages = commitIndexDBLeastPages;
	}

	public int getFlushIndexDBLeastPages() {
		return flushIndexDBLeastPages;
	}

	public int getCommitIndexToroughInterval() {
		return commitIndexToroughInterval;
	}

	public void setCommitIndexToroughInterval(int commitIndexToroughInterval) {
		this.commitIndexToroughInterval = commitIndexToroughInterval;
	}

	public int getFlushIndexThoroughInterval() {
		return flushIndexThoroughInterval;
	}

	public void setFlushIndexThoroughInterval(int flushIndexThoroughInterval) {
		this.flushIndexThoroughInterval = flushIndexThoroughInterval;
	}

	public int getRedeleteHangedFileInterval() {
		return redeleteHangedFileInterval;
	}

	public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
		this.redeleteHangedFileInterval = redeleteHangedFileInterval;
	}

	public int getMaxTransferBytesOnMessageInMemory() {
		return maxTransferBytesOnMessageInMemory;
	}

	public void setMaxTransferBytesOnMessageInMemory(
			int maxTransferBytesOnMessageInMemory) {
		this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
	}

	public int getMaxTransferCountOnMessageInMemory() {
		return maxTransferCountOnMessageInMemory;
	}

	public void setMaxTransferCountOnMessageInMemory(
			int maxTransferCountOnMessageInMemory) {
		this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
	}

	public int getMaxTransferBytesOnMessageInDisk() {
		return maxTransferBytesOnMessageInDisk;
	}

	public void setMaxTransferBytesOnMessageInDisk(
			int maxTransferBytesOnMessageInDisk) {
		this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
	}

	public int getMaxTransferCountOnMessageInDisk() {
		return maxTransferCountOnMessageInDisk;
	}

	public void setMaxTransferCountOnMessageInDisk(
			int maxTransferCountOnMessageInDisk) {
		this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
	}

	public int getAccessMessageInMemoryMaxRatio() {
		return accessMessageInMemoryMaxRatio;
	}

	public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
		this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
	}

	public boolean isCleanFileForciblyEnable() {
		return cleanFileForciblyEnable;
	}

	public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
		this.cleanFileForciblyEnable = cleanFileForciblyEnable;
	}

	public int getStoreCheckPointFlushInterval() {
		return storeCheckPointFlushInterval;
	}

	public void setStoreCheckPointFlushInterval(int storeCheckPointFlushInterval) {
		this.storeCheckPointFlushInterval = storeCheckPointFlushInterval;
	}

	public boolean isWarmMapedFileEnable() {
		return warmMapedFileEnable;
	}

	public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
		this.warmMapedFileEnable = warmMapedFileEnable;
	}

	public int getFlushLeastPagesWhenWarmMapedFile() {
		return flushLeastPagesWhenWarmMapedFile;
	}

	public void setFlushLeastPagesWhenWarmMapedFile(
			int flushLeastPagesWhenWarmMapedFile) {
		this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
	}

	public int getMaxMapedfileUntouchTime() {
		return maxMapedfileUntouchTime;
	}

	public void setMaxMapedfileUntouchTime(int maxMapedfileUntouchTime) {
		StoreConfig.maxMapedfileUntouchTime = maxMapedfileUntouchTime;
	}

	public boolean isTransientStorePoolEnable() {
		return transientStorePoolEnable;
	}

	public void setTransientStorePoolEnable(boolean transientStorePoolEnable) {
		this.transientStorePoolEnable = transientStorePoolEnable;
	}

	public int getTransientStorePoolSize() {
		return transientStorePoolSize;
	}

	public void setTransientStorePoolSize(int transientStorePoolSize) {
		this.transientStorePoolSize = transientStorePoolSize;
	}

	public int getTransientStoreIndexDBPoolSize() {
		return transientStoreIndexDBPoolSize;
	}

	public void setTransientStoreIndexDBPoolSize(int transientStoreIndexDBPoolSize) {
		this.transientStoreIndexDBPoolSize = transientStoreIndexDBPoolSize;
	}

	public boolean isFastFailIfNoBufferInStorePool() {
		return fastFailIfNoBufferInStorePool;
	}

	public void setFastFailIfNoBufferInStorePool(boolean fastFailIfNoBufferInStorePool) {
		this.fastFailIfNoBufferInStorePool = fastFailIfNoBufferInStorePool;
	}

	public int getMaxIndexNum() {
		return maxIndexNum;
	}

	public void setMaxIndexNum(int maxIndexNum) {
		this.maxIndexNum = maxIndexNum;
	}

	public int getCommitIntervalPhysicLog() {
		return commitIntervalPhysicLog;
	}

	public void setCommitIntervalPhysicLog(int commitIntervalPhysicLog) {
		this.commitIntervalPhysicLog = commitIntervalPhysicLog;
	}

	public void setFlushIndexDBLeastPages(int flushIndexDBLeastPages) {
		this.flushIndexDBLeastPages = flushIndexDBLeastPages;
	}

	public String getStorePathPhysicLog() {
		return StorePathConfigHelper.getStorePathPhysicLog(this.storeRootPath);
	}

	public String getStorePathIndexDB() {
		return StorePathConfigHelper.getStorePathIndexDB(this.storeRootPath);
	}

	public String getStorePathVarStore() {
		return StorePathConfigHelper.getStorePathVarStore(this.storeRootPath);
	}

	public String getAbortFile() {
		return StorePathConfigHelper.getAbortFile(this.storeRootPath);
	}

	public String getStoreCheckpoint() {
		return StorePathConfigHelper.getStoreCheckpoint(this.storeRootPath);
	}

	public String getStoreRootPath() {
		return storeRootPath;
	}

	/**
	 * Write the value in Properties to Object
	 *
	 * @throws Exception
	 */
	private static void properties2Object(final Properties p, final Object object) throws Exception {
		Method[] methods = object.getClass().getMethods();
		for (Method method : methods) {
			String mn = method.getName();
			if (mn.startsWith("set")) {
				try {
					String tmp = mn.substring(4);
					String first = mn.substring(3, 4);

					String key = first.toLowerCase() + tmp;
					String property = p.getProperty(key);
					if (property != null) {
						Class<?>[] pt = method.getParameterTypes();
						if (pt != null && pt.length > 0) {
							String cn = pt[0].getSimpleName();
							Object arg = null;
							if ("int".equals(cn) || "Integer".equals(cn)) {
								arg = Integer.parseInt(property);
							} else if ("long".equals(cn) || "Long".equals(cn)) {
								arg = Long.parseLong(property);
							} else if ("double".equals(cn) || "Double".equals(cn)) {
								arg = Double.parseDouble(property);
							} else if ("boolean".equals(cn) || "Boolean".equals(cn)) {
								arg = Boolean.parseBoolean(property);
							} else if ("String".equals(cn)) {
								arg = property;
							} else if ("FlushDiskType".equals(cn)) {
								arg = Enum.valueOf(FlushDiskType.class, property);
							} else {
								continue;
							}
							method.invoke(object, new Object[]{arg});
						}
					}
				} catch(Exception e) {
					throw e;
				}
			}
		}
	}
}
