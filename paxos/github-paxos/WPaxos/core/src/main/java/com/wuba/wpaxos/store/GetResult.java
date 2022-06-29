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
package com.wuba.wpaxos.store;

import java.nio.ByteBuffer;

import com.wuba.wpaxos.store.pagecache.MapedFile;

/**
 * get bytebuffer slice of mapedFile from startOffset
 */
public class GetResult {
    // From which absolute offset in the queue to start
    private final long startOffset;
    // position starts from 0
    private final ByteBuffer byteBuffer;
    // Effective data size
    private int size;
    // Used to release memory
    private MapedFile mapedFile;

    public GetResult(long startOffset, ByteBuffer byteBuffer, int size, MapedFile mapedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mapedFile = mapedFile;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    public MapedFile getMapedFile() {
        return mapedFile;
    }

    /**
     * This method can only be called once, repeated calls are invalid
     */
    public synchronized void release() {
        if (this.mapedFile != null) {
            this.mapedFile.release();
            this.mapedFile = null;
        }
    }

    public long getStartOffset() {
        return startOffset;
    }
}
