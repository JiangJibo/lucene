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

import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

abstract class TermsHashPerField implements Comparable<TermsHashPerField> {

    private static final int HASH_INIT_SIZE = 4;

    final TermsHash termsHash;

    final TermsHashPerField nextPerField;
    protected final DocumentsWriterPerThread.DocState docState;
    protected final FieldInvertState fieldState;
    TermToBytesRefAttribute termAtt;
    protected TermFrequencyAttribute termFreqAtt;

    // Copied from our perThread
    final IntBlockPool intPool;
    final ByteBlockPool bytePool;
    final ByteBlockPool termBytePool;

    /**
     * 每个Field的每个term要存储多少个stream(数据),比如freq是一个prox(位置信息,position)+offset是一个
     * {@link TermVectorsConsumerPerField} 要存freq+offset, 所以为 2
     * {@link FreqProxTermsWriterPerField} 的IndexOptions为 {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}时是2,否则为1
     */
    final int streamCount;

    /**
     * 默认值2* {@link #streamCount}
     *
     * @see {@link TermVectorsConsumerPerField#TermVectorsConsumerPerField(FieldInvertState, TermVectorsConsumer, FieldInfo)}
     */
    final int numPostingInt;

    protected final FieldInfo fieldInfo;

    final BytesRefHash bytesHash;

    ParallelPostingsArray postingsArray;
    private final Counter bytesUsed;

    /**
     * 每个Field的每个term要存储多少个stream(数据),比如freq是一个prox+offset是一个
     * streamCount: how many streams this field stores per term.
     * E.g. doc(+freq) is 1 stream, prox+offset is a second.
     *
     * {@link TermVectorsConsumerPerField}
     *
     * @param streamCount  有值1 和 2
     * @param fieldState
     * @param termsHash
     * @param nextPerField
     * @param fieldInfo
     * @see
     */

    public TermsHashPerField(int streamCount, FieldInvertState fieldState, TermsHash termsHash, TermsHashPerField nextPerField, FieldInfo fieldInfo) {
        intPool = termsHash.intPool;
        bytePool = termsHash.bytePool;
        termBytePool = termsHash.termBytePool;
        docState = termsHash.docState;
        this.termsHash = termsHash;
        bytesUsed = termsHash.bytesUsed;
        this.fieldState = fieldState;
        this.streamCount = streamCount;
        numPostingInt = 2 * streamCount;
        this.fieldInfo = fieldInfo;
        this.nextPerField = nextPerField;
        PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
        bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
    }

    void reset() {
        bytesHash.clear(false);
        if (nextPerField != null) {
            nextPerField.reset();
        }
    }

    public void initReader(ByteSliceReader reader, int termID, int stream) {
        assert stream < streamCount;
        int intStart = postingsArray.intStarts[termID];
        final int[] ints = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
        final int upto = intStart & IntBlockPool.INT_BLOCK_MASK;
        reader.init(bytePool,
            postingsArray.byteStarts[termID] + stream * ByteBlockPool.FIRST_LEVEL_SIZE,
            ints[upto + stream]);
    }

    int[] sortedTermIDs;

    /**
     * Collapse the hash table and sort in-place; also sets
     * this.sortedTermIDs to the results
     */
    public int[] sortPostings() {
        sortedTermIDs = bytesHash.sort();
        return sortedTermIDs;
    }

    private boolean doNextCall;

    // Secondary entry point (for 2nd & subsequent TermsHash),
    // because token text has already been "interned" into
    // textStart, so we hash by textStart.  term vectors use
    // this API.
    public void add(int textStart) throws IOException {
        int termID = bytesHash.addByPoolOffset(textStart);
        if (termID >= 0) {      // New posting
            // First time we are seeing this token since we last
            // flushed the hash.
            // Init stream slices
            if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
                intPool.nextBuffer();
            }

            if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE) {
                bytePool.nextBuffer();
            }

            intUptos = intPool.buffer;
            intUptoStart = intPool.intUpto;
            intPool.intUpto += streamCount;

            postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

            for (int i = 0; i < streamCount; i++) {
                final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
                intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
            }
            postingsArray.byteStarts[termID] = intUptos[intUptoStart];

            newTerm(termID);

        } else {
            termID = (-termID) - 1;
            int intStart = postingsArray.intStarts[termID];
            intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
            intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
            addTerm(termID);
        }
    }

    /**
     * Called once per inverted token.  This is the primary
     * entry point (for first TermsHash); postings use this
     * API.
     */
    void add() throws IOException {
        // We are first in the chain so we must "intern" the
        // term text into textStart address
        // Get the text & hash of this term.
        // termID :也就是此term在当前field里的序号
        int termID = bytesHash.add(termAtt.getBytesRef());
        // 打印数据
        System.out.println("add term=" + termAtt.getBytesRef().utf8ToString() + " doc=" + docState.docID + " termID=" + termID);
        // New posting, 也就是新增
        if (termID >= 0) {
            bytesHash.byteStart(termID);
            // Init stream slices, 如果当前buffer在加上待提交的超过了最大长度,新生成一个buffer,指向下一个buffer
            if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
                intPool.nextBuffer();
            }

            if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE) {
                bytePool.nextBuffer();
            }

            intUptos = intPool.buffer;
            intUptoStart = intPool.intUpto;
            intPool.intUpto += streamCount;

            // 提交数组里第几个term的在 IntBlockPool#buffers 里的总的数据起始位置
            postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

            for (int i = 0; i < streamCount; i++) {
                // 获取bytePool 里的buffer的offset
                final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
                // intPool的 intUpto+i 指向bytePool的buffers里的offset
                intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
            }
            postingsArray.byteStarts[termID] = intUptos[intUptoStart];

            newTerm(termID);

        }
        // 当前field里此term不是第一次出现
        else {
            termID = (-termID) - 1;
            int intStart = postingsArray.intStarts[termID];
            intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
            intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
            addTerm(termID);
        }

        if (doNextCall) {
            nextPerField.add(postingsArray.textStarts[termID]);
        }
    }

    /**
     * 指向 {@link #intPool} intPool.buffer
     *
     * @see {@link #add()}
     */
    int[] intUptos;
    /**
     * 当前数据在 intPool.buffe 中的最大位置,nextBuffer(..)中初始化为0
     */
    int intUptoStart;

    void writeByte(int stream, byte b) {
        int upto = intUptos[intUptoStart + stream];
        // upto >> ByteBlockPool.BYTE_BLOCK_SHIFT 相当于 upto/8192, 也就是定位第几个buffer
        byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
        assert bytes != null;
        // upto 对 8192*4 取余数
        int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
        if (bytes[offset] != 0) {
            // End of slice; allocate a new one
            offset = bytePool.allocSlice(bytes, offset);
            bytes = bytePool.buffer;
            intUptos[intUptoStart + stream] = offset + bytePool.byteOffset;
        }
        bytes[offset] = b;
        (intUptos[intUptoStart + stream])++;
    }

    public void writeBytes(int stream, byte[] b, int offset, int len) {
        // TODO: optimize
        final int end = offset + len;
        for (int i = offset; i < end; i++) { writeByte(stream, b[i]); }
    }

    /**
     * @param stream
     * @param i      16进制的数据
     */
    void writeVInt(int stream, int i) {
        assert stream < streamCount;
        // 如果一个field里term太多,那么序号就会超过128,所以要缩小下，写两个字节，因为这个数据时UTF-16的，所以int最多也就占2个字节
        while ((i & ~0x7F) != 0) {
            writeByte(stream, (byte)((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte(stream, (byte)i);
    }

    private static final class PostingsBytesStartArray extends BytesStartArray {

        private final TermsHashPerField perField;
        private final Counter bytesUsed;

        private PostingsBytesStartArray(
            TermsHashPerField perField, Counter bytesUsed) {
            this.perField = perField;
            this.bytesUsed = bytesUsed;
        }

        @Override
        public int[] init() {
            if (perField.postingsArray == null) {
                perField.postingsArray = perField.createPostingsArray(2);
                perField.newPostingsArray();
                bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
            }
            return perField.postingsArray.textStarts;
        }

        @Override
        public int[] grow() {
            ParallelPostingsArray postingsArray = perField.postingsArray;
            final int oldSize = perField.postingsArray.size;
            postingsArray = perField.postingsArray = postingsArray.grow();
            perField.newPostingsArray();
            bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
            return postingsArray.textStarts;
        }

        @Override
        public int[] clear() {
            if (perField.postingsArray != null) {
                bytesUsed.addAndGet(-(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
                perField.postingsArray = null;
                perField.newPostingsArray();
            }
            return null;
        }

        @Override
        public Counter bytesUsed() {
            return bytesUsed;
        }
    }

    @Override
    public int compareTo(TermsHashPerField other) {
        return fieldInfo.name.compareTo(other.fieldInfo.name);
    }

    /**
     * Finish adding all instances of this field to the
     * current document.
     */
    void finish() throws IOException {
        if (nextPerField != null) {
            nextPerField.finish();
        }
    }

    /**
     * Start adding a new field instance; first is true if
     * this is the first time this field name was seen in the
     * document.
     */
    boolean start(IndexableField field, boolean first) {
        termAtt = fieldState.termAttribute;
        termFreqAtt = fieldState.termFreqAttribute;
        if (nextPerField != null) {
            doNextCall = nextPerField.start(field, first);
        }

        return true;
    }

    /**
     * Called when a term is seen for the first time.
     */
    abstract void newTerm(int termID) throws IOException;

    /**
     * Called when a previously seen term is seen again.
     */
    abstract void addTerm(int termID) throws IOException;

    /**
     * Called when the postings array is initialized or
     * resized.
     */
    abstract void newPostingsArray();

    /**
     * Creates a new postings array of the specified size.
     */
    abstract ParallelPostingsArray createPostingsArray(int size);
}
