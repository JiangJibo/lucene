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
     * 每个Field的每个term要存储多少个数据块,比如freq是一个,prox(位置信息,position)+offset是一个,每个数据块在bytePool中对应一个片段
     * {@link TermVectorsConsumerPerField} 要存freq+offset, 所以为 2
     * {@link FreqProxTermsWriterPerField} 的IndexOptions为 {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}时是2,否则为1
     */
    final int streamCount;

    /**
     * 2* {@link #streamCount}, 一个term对应1或者2个int数据, 一个int对应5个字节
     */
    final int numPostingInt;

    protected final FieldInfo fieldInfo;

    final BytesRefHash bytesHash;

    /**
     * 存储termID在 {@link #intPool} 和 {@link #bytePool} 中的数据位置
     */
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

    /**
     * 初始化读取freq,porx,offset的读取器
     *
     * @param reader
     * @param termID
     * @param stream
     */
    public void initReader(ByteSliceReader reader, int termID, int stream) {
        assert stream < streamCount;
        int intStart = postingsArray.intStarts[termID];
        final int[] ints = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
        final int upto = intStart & IntBlockPool.INT_BLOCK_MASK;
        reader.init(bytePool, postingsArray.byteStarts[termID] + stream * ByteBlockPool.FIRST_LEVEL_SIZE, ints[upto + stream]);
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
     *
     * 在ByteBlockPool中，文档号和词频(freq)信息是应用或然跟随原则写到一个块中去的，而位置信息(prox)是写入到另一个块中去的，
     * 对于同一个词，这两块的偏移量保存在IntBlockPool中。因而在IntBlockPool中，每一个词都有两个int，
     * 0：第0个表示docid +freq在ByteBlockPool中的偏移量，
     * 1：第1个表示prox在ByteBlockPool中的偏移量。
     * 在写入docid + freq信息的时候，调用termsHashPerField.writeVInt(0, p.lastDocCode)，
     * 第一个参数表示向此词的第0个偏移量写入；在写入prox信息的时候，调用termsHashPerField.writeVInt(1, (proxCode<<1)|1)，第一个参数表示向此词的第1个偏移量写入。
     */
    void add() throws IOException {
        // We are first in the chain so we must "intern" the
        // term text into textStart address
        // Get the text & hash of this term.
        // termID :也就是此term在当前field里的序号,  termAtt.getBytesRef() : 也就是term的值,以字节形式展示
        // termID正常是递增的,但是如果这个term之前在此Field里存储过,那么会返回之前的 -(第一次termId + 1)
        // byteHash存储term的字节长度和字节数据, length(1,2字节) + body
        int termID = bytesHash.add(termAtt.getBytesRef());
        // 打印数据
        System.out.println("add term=" + termAtt.getBytesRef().utf8ToString() + " doc=" + docState.docID + " termID=" + termID);
        // New posting, 也就是此term是当前field里第一次写入
        if (termID >= 0) {
            bytesHash.byteStart(termID);
            // Init stream slices, 如果当前buffer在加上待提交的超过了最大长度,新生成一个buffer,指向下一个buffer
            if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
                intPool.nextBuffer();
            }
            // 一个term对应1或者2个int数据, 一个int对应5个字节
            if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE) {
                bytePool.nextBuffer();
            }
            // 指向当前最新的buffer
            intUptos = intPool.buffer;
            // 指向最新buffer里的最新数据位置
            intUptoStart = intPool.intUpto;
            // 最新buffer里的数据位置+1/2, 一个用于存储freq, 一个存储prox和offset
            intPool.intUpto += streamCount;

            // 提交数组里第几个term的在 IntBlockPool#buffers 里的总的数据起始位置
            postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

            // 在intPool里分配1/2个位置, 存储的是bytePool里的字节起始位置, 每个int对应5个字节, 第5个存16(0x10)来做分隔开
            for (int i = 0; i < streamCount; i++) {
                // 在bytePool里分配5个字节,返回第一个字节的位置
                final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
                // intPool的 intUpto+i 指向bytePool的buffers里的offset
                intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
            }
            // byteStarts 在term的位置上存储当前term 执行 intPool里当前数据的起始位置, intPool又指向bytePool的数据位置
            postingsArray.byteStarts[termID] = intUptos[intUptoStart];

            newTerm(termID);

        }
        // 当前field里此term不是第一次出现
        else {
            termID = (-termID) - 1;
            int intStart = postingsArray.intStarts[termID];
            // 拿到这个term第一次存的intPool的位置
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
     * 当前数据在 intPool.buffer 中的下一个数据可以写入的位置
     * 当前block里的数据起始位置, intUptoStart+0: freq的写入位置, intUptoStart+1: prox和offset的写入位置
     * 每写一个数据, intUptos[intUptoStart + stream] 位置的值就会自增1,也就是指向的bytePool里的位置+1
     *
     * @see #writeByte(int, byte) 的末尾行
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
        // intUptos intUptoStart + stream 这个位置的元素自增1,这样下次再次写入同一个block时,能自动指向bytePool里的下一个字节,参考此方法的第一行
        (intUptos[intUptoStart + stream])++;
    }

    public void writeBytes(int stream, byte[] b, int offset, int len) {
        // TODO: optimize
        final int end = offset + len;
        for (int i = offset; i < end; i++) { writeByte(stream, b[i]); }
    }

    /**
     * 先写低位再写高位,按顺序, 也就是存储的顺序低位在前,高位在后
     *
     * @param stream
     * @param i      原始数据向左移动一位后的数据,假设 i = 10000
     */
    void writeVInt(int stream, int i) {
        assert stream < streamCount;
        // 如果一个field里term太多,那么序号就会超过128,所以需要用多个字节来表示,同时最高位用1表示后续还有跟着的字节
        // 将i抹去7位后面的数据,将低7位置为0,之后得到的高位不为0, 比如 129 & ~0x7F = 128, 257 & ~0x7F = 256,
        while ((i & ~0x7F) != 0) {
            // 先写数据低位, 10000%128 = 16,  16 | 0x80 = 144 , 144转为byte = -112, 也就是从-128往正的方向走 16(144-128)
            // 用 - 号指定后续的字节是当前数字的
            writeByte(stream, (byte)((i & 0x7f) | 0x80));
            // 然后将i向右移动7位,也就是抹去低7位
            i >>>= 7;
        }
        // 最后写入, 如果前一个字节是负的,此字节写入: 10000%128 = 78
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
