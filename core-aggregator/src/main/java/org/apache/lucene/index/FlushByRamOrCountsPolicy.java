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

import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;

/**
 * Default {@link FlushPolicy} implementation that flushes new segments based on
 * RAM used and document count depending on the IndexWriter's
 * {@link IndexWriterConfig}. It also applies pending deletes based on the
 * number of buffered delete terms.
 *
 * <ul>
 * <li>
 * {@link #onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)}
 * - applies pending delete operations based on the global number of buffered
 * delete terms if the consumed memory is greater than {@link IndexWriterConfig#getRAMBufferSizeMB()}</li>.
 * <li>
 * {@link #onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)}
 * - flushes either on the number of documents per
 * {@link DocumentsWriterPerThread} (
 * {@link DocumentsWriterPerThread#getNumDocsInRAM()}) or on the global active
 * memory consumption in the current indexing session iff
 * {@link IndexWriterConfig#getMaxBufferedDocs()} or
 * {@link IndexWriterConfig#getRAMBufferSizeMB()} is enabled respectively</li>
 * <li>
 * {@link #onUpdate(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)}
 * - calls
 * {@link #onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)}
 * and
 * {@link #onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThreadPool.ThreadState)}
 * in order</li>
 * </ul>
 * All {@link IndexWriterConfig} settings are used to mark
 * {@link DocumentsWriterPerThread} as flush pending during indexing with
 * respect to their live updates.
 * <p>
 * If {@link IndexWriterConfig#setRAMBufferSizeMB(double)} is enabled, the
 * largest ram consuming {@link DocumentsWriterPerThread} will be marked as
 * pending iff the global active RAM consumption is {@code >=} the configured max RAM
 * buffer.
 */
class FlushByRamOrCountsPolicy extends FlushPolicy {

    /**
     * 当doc删除时
     * 设置 {@link DocumentsWriterFlushControl#fullFlush} 来表示当前需要处理所有的删除和更新
     *
     * @param control
     * @param state
     */
    @Override
    public void onDelete(DocumentsWriterFlushControl control, ThreadState state) {

        // 默认是根据内存使用量, 如果doc的变更超过16M
        if ((flushOnRAM() && control.getDeleteBytesUsed() > 1024 * 1024 * indexWriterConfig.getRAMBufferSizeMB())) {
            // 设置需要处理所有的删除
            control.setApplyAllDeletes();
            if (infoStream.isEnabled("FP")) {
                infoStream.message("FP",
                    "force apply deletes bytesUsed=" + control.getDeleteBytesUsed() + " vs ramBufferMB="
                        + indexWriterConfig.getRAMBufferSizeMB());
            }
        }
    }

    /**
     * 当doc新增时
     *
     * @param control
     * @param state
     */
    @Override
    public void onInsert(DocumentsWriterFlushControl control, ThreadState state) {

        if (flushOnDocCount() && state.dwpt.getNumDocsInRAM() >= indexWriterConfig.getMaxBufferedDocs()) {
            // Flush this state by num docs
            control.setFlushPending(state);
        } else if (flushOnRAM()) {// flush by RAM, default true
            // 内存限制,默认16M
            final long limit = (long)(indexWriterConfig.getRAMBufferSizeMB() * 1024.d * 1024.d);
            // 当前未flush的内存量
            final long totalRam = control.activeBytes() + control.getDeleteBytesUsed();
            // 如果超过限制, 触发flush
            if (totalRam >= limit) {
                if (infoStream.isEnabled("FP")) {
                    infoStream.message("FP",
                        "trigger flush: activeBytes=" + control.activeBytes() + " deleteBytes=" + control
                            .getDeleteBytesUsed() + " vs limit=" + limit);
                }
                markLargestWriterPending(control, state, totalRam);
            }
        }
    }

    /**
     * 指定未Flush数据量最多的DWPT将被Flush
     * Marks the most ram consuming active {@link DocumentsWriterPerThread} flush
     * pending
     */
    protected void markLargestWriterPending(DocumentsWriterFlushControl control,
        ThreadState perThreadState, final long currentBytesPerThread) {
        // 指定要Flush那个DWPT
        control.setFlushPending(findLargestNonPendingWriter(control, perThreadState));
    }

    /**
     * 默认是false,也就是flush不是根据doc的数量来的
     * Returns <code>true</code> if this {@link FlushPolicy} flushes on
     * {@link IndexWriterConfig#getMaxBufferedDocs()}, otherwise
     * <code>false</code>.
     */
    protected boolean flushOnDocCount() {
        return indexWriterConfig.getMaxBufferedDocs() != IndexWriterConfig.DISABLE_AUTO_FLUSH;
    }

    /**
     * 默认true
     * Returns <code>true</code> if this {@link FlushPolicy} flushes on
     * {@link IndexWriterConfig#getRAMBufferSizeMB()}, otherwise
     * <code>false</code>.
     */
    protected boolean flushOnRAM() {
        //                                             = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;
        return indexWriterConfig.getRAMBufferSizeMB() != IndexWriterConfig.DISABLE_AUTO_FLUSH;
    }
}
