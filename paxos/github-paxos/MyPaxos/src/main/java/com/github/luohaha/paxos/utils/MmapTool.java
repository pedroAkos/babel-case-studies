package com.github.luohaha.paxos.utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * Implementation of mmap basic functions
 * @author luoyixin
 *
 */
public class MmapTool {
	private RandomAccessFile raf;
	private FileChannel channel;
	private MappedByteBuffer buffer;
	private MappedByteBuffer readBuffer;
	// The size of the space allocated for each request
	private final int CHUNK_SIZE = 1024;
	// starting point
	private int start = 0;
	// Final location
	private int end = 0;

	public MmapTool(String addr) throws IOException {
		this.raf = new RandomAccessFile(addr, "rw");
		this.channel = this.raf.getChannel();
		this.channel.force(true);
		this.end += CHUNK_SIZE;
		this.buffer = channel.map(MapMode.READ_WRITE, this.start, this.end - this.start);
	}

	/**
	 * Set the file length to 0, clear the file
	 * @throws IOException
	 */
	public void clear() throws IOException {
		this.raf.setLength(0);
		this.start = this.end = 0;
		extendMemory();
	}

	/**
	 * Write data to file
	 * @param data
	 * @throws IOException
	 */
	public void write(byte[] data) throws IOException {
		while (data.length >= this.end - this.start) {
			extendMemory();
		}
		this.buffer.put(data);
		this.start += data.length;
	}

	/**
	 * Read data
	 * @return
	 * @throws IOException
	 */
	public byte[] read() throws IOException {
		this.readBuffer = channel.map(MapMode.READ_ONLY, 0, this.raf.length());
		byte[] data = new byte[(int) this.raf.length()];
		this.readBuffer.get(data);
		return data;
	}

	/**
	 * Expand storage
	 * @throws IOException
	 */
	private void extendMemory() throws IOException {
		this.end += CHUNK_SIZE;
		this.buffer = channel.map(MapMode.READ_WRITE, start, end - start);
	}

}
