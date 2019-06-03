/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import org.apache.lucene.util.ArrayUtil;

/**
 * 并行提交数组
 */
class ParallelPostingsArray {

  /**
   * 3*4 = 12
   */
  final static int BYTES_PER_POSTING = 3 * Integer.BYTES;

  final int size;
  /**
   * 本来是用来记录term本身在ByteBlockPool中的起始位置的，建索引的时候没有用到这个字段。
   */
  final int[] textStarts;
  /**
   * 提交数组里第几个term的在 IntBlockPool#buffers 里的总的数据起始位置
   */
  final int[] intStarts;
  /**
   * 在term的位置上存储当前term 指向 intPool里当前数据的起始位置, intPool又指向bytePool的数据位置
   */
  final int[] byteStarts;

  ParallelPostingsArray(final int size) {
    this.size = size;
    textStarts = new int[size];
    intStarts = new int[size];
    byteStarts = new int[size];
  }

  int bytesPerPosting() {
    return BYTES_PER_POSTING;
  }

  ParallelPostingsArray newInstance(int size) {
    return new ParallelPostingsArray(size);
  }

  final ParallelPostingsArray grow() {
    int newSize = ArrayUtil.oversize(size + 1, bytesPerPosting());
    ParallelPostingsArray newArray = newInstance(newSize);
    copyTo(newArray, size);
    return newArray;
  }

  void copyTo(ParallelPostingsArray toArray, int numToCopy) {
    System.arraycopy(textStarts, 0, toArray.textStarts, 0, numToCopy);
    System.arraycopy(intStarts, 0, toArray.intStarts, 0, numToCopy);
    System.arraycopy(byteStarts, 0, toArray.byteStarts, 0, numToCopy);
  }
}
