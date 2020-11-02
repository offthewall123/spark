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

package org.apache.spark.unsafe.map;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;

public class PMemReaderForBytesToBytesMap extends UnsafeSorterIterator implements Closeable {

  private int numRecords;
  private int readingPageIndex = 0;
  int uaoSize = UnsafeAlignedOffset.getUaoSize();
  private int numRecordsRemaining;
  private int recordLength;
  private int keyPrefix;
  private MemoryBlock pMemPage = null;
  private int readedRecordsInCurrentPage = 0;
  private int numRecordsInpage = 0;
  private long offset = 0;

  private Object baseObject;

  private LinkedList<MemoryBlock> pMemPages;

  public PMemReaderForBytesToBytesMap(LinkedList<MemoryBlock> pMemPages, int numRecords) {
    this.pMemPages = pMemPages;
    this.numRecords = numRecords;
  }

  @Override
  public void close() throws IOException {
    // do nothing here
  }

  @Override
  public boolean hasNext() {
    return (numRecordsRemaining > 0);
  }

  @Override
  public void loadNext() throws IOException {
    assert (readingPageIndex <= pMemPages.size())
            : "Illegal state: Pages finished read but hasNext() is true.";

    if(pMemPage == null || readedRecordsInCurrentPage == numRecordsInpage) {
      // read records from each page
      offset = pMemPage.getBaseOffset();
      pMemPage = pMemPages.get(readingPageIndex++);
      readedRecordsInCurrentPage = 0;
      numRecordsInpage = UnsafeAlignedOffset.getSize(pMemPage.getBaseObject(), offset);
      offset += uaoSize;
    }
    // (total length) (key length) (key) (value) (8 byte pointer to next value)
    recordLength = UnsafeAlignedOffset.getSize(pMemPage.getBaseObject(), offset);
    offset += uaoSize;
    int keyLen = UnsafeAlignedOffset.getSize(pMemPage.getBaseObject(), offset);
    offset += uaoSize;
    // dataLen + pointerLen
    int dataLen = recordLength - uaoSize * 2;
    Platform.copyMemory(null, offset, baseObject, Platform.BYTE_ARRAY_OFFSET, dataLen);
    offset += dataLen;
    readedRecordsInCurrentPage ++;
    numRecordsRemaining --;
  }

  @Override
  public Object getBaseObject() {
    return baseObject;
  }

  @Override
  public long getBaseOffset() {
    return Platform.BYTE_ARRAY_OFFSET;
  }

  @Override
  public int getRecordLength() {
    return recordLength;
  }

  @Override
  public long getKeyPrefix() {
    // no keyprefix for BytesToBytesMap
    return 0;
  }

  @Override
  public int getNumRecords() {
    return numRecords;
  }
}
