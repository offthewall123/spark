/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.map.PMemReaderForBytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryBlock;

import java.util.HashMap;
import java.util.LinkedList;

public final class PMemWriter {
    private final ShuffleWriteMetrics writeMetrics;
    private final TaskMemoryManager taskMemoryManager;
    private final LinkedList<MemoryBlock> allocatedPMemPages = new LinkedList<>();
    private LongArray sortedArray;
    private HashMap<MemoryBlock, MemoryBlock> pageMap = new HashMap<>();
    private int numRecordsWritten;

    public PMemWriter(
            ShuffleWriteMetrics writeMetrics,
            TaskMemoryManager taskMemoryManager, int numRecords) {
        this.writeMetrics = writeMetrics;
        this.taskMemoryManager = taskMemoryManager;
        this.numRecordsWritten = numRecords;
    }

    public boolean dumpPageToPMem(MemoryBlock page) {
        MemoryBlock pMemBlock = taskMemoryManager.allocatePMemPage(page.size());
        if (pMemBlock != null) {
            Platform.copyMemory(page.getBaseObject(), page.getBaseOffset(), null, pMemBlock.getBaseOffset(), page.size());
            writeMetrics.incBytesWritten(page.size());
            allocatedPMemPages.add(pMemBlock);
            pageMap.put(page, pMemBlock);
            return true;
        }
        return false;
    }

  // for BytesToBytesMap
  public boolean bytesToBytesMapDumpPageToPMem(MemoryBlock page, int numRecordsInPage) {
    MemoryBlock pMemBlock = taskMemoryManager.allocatePMemPage(page.size());
    if (pMemBlock != null) {
      Platform.copyMemory(page.getBaseObject(), page.getBaseOffset(), null, pMemBlock.getBaseOffset(), page.size());
      writeMetrics.incBytesWritten(page.size());
      allocatedPMemPages.add(pMemBlock);
      numRecordsWritten += numRecordsInPage;
      pageMap.put(page, pMemBlock);
      return true;
    }
    return false;
  }

    public int getNumRecordsWritten() {
        return numRecordsWritten;
    }

    // for free PMem page
    public LinkedList<MemoryBlock> getAllocatedPMemPages() { return allocatedPMemPages; }

    public PMemReaderForUnsafeExternalSorter getPMemReaderForUnsafeExternalSorter() {
        return new PMemReaderForUnsafeExternalSorter(sortedArray, numRecordsWritten);
    }

    public PMemReaderForBytesToBytesMap getPMemReaderForBytesToBytesMap() {
      return new PMemReaderForBytesToBytesMap(allocatedPMemPages, numRecordsWritten);
    }

    public void updateLongArray(LongArray sortedArray, int numRecords) {
        int i = 0;
        while (i < numRecords * 2){
            // update recordPointer in this array
            long originalRecordPointer = sortedArray.get(i);
            MemoryBlock page = taskMemoryManager.getOriginalPage(originalRecordPointer);
            long offset = taskMemoryManager.getOffsetInPage(originalRecordPointer) - page.getBaseOffset();
            MemoryBlock pMemBlock = pageMap.get(page);
            long pMemOffset = pMemBlock.getBaseOffset() + offset;
            sortedArray.set(i, pMemOffset);
            i += 2;
        }
        this.sortedArray = sortedArray;
    }
    public LongArray getSortedArray() {
        return sortedArray;
    }
}
