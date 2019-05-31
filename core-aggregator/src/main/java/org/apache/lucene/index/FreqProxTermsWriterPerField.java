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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField {

    private FreqProxPostingsArray freqProxPostingsArray;

    /**
     * 是否写出现频率
     */
    final boolean hasFreq;
    /**
     * 是否写位置信息
     */
    final boolean hasProx;
    /**
     * 是否写位置信息的偏移量
     */
    final boolean hasOffsets;

    PayloadAttribute payloadAttribute;
    OffsetAttribute offsetAttribute;
    long sumTotalTermFreq;
    long sumDocFreq;

    // How many docs have this field:
    int docCount;

    /**
     * Set to true if any token had a payload in the current
     * segment.
     */
    boolean sawPayloads;

    /**
     * @param invertState
     * @param termsHash
     * @param fieldInfo
     * @param nextPerField
     */
    public FreqProxTermsWriterPerField(FieldInvertState invertState, TermsHash termsHash, FieldInfo fieldInfo, TermsHashPerField nextPerField) {
        // 如果既要存储Freq,又要存储Prox, 那么需要两个stream
        super(fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 ? 2 : 1, invertState, termsHash, nextPerField, fieldInfo);
        IndexOptions indexOptions = fieldInfo.getIndexOptions();
        assert indexOptions != IndexOptions.NONE;
        // 判断是否写freq
        hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
        // 判断是否写位置信息
        hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        // 判断是否写offset
        hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    void finish() throws IOException {
        super.finish();
        sumDocFreq += fieldState.uniqueTermCount;
        sumTotalTermFreq += fieldState.length;
        if (fieldState.length > 0) {
            docCount++;
        }

        if (sawPayloads) {
            fieldInfo.setStorePayloads();
        }
    }

    @Override
    boolean start(IndexableField f, boolean first) {
        super.start(f, first);
        payloadAttribute = fieldState.payloadAttribute;
        offsetAttribute = fieldState.offsetAttribute;
        return true;
    }

    /**
     * 写位置信息
     *
     * @param termID   当前term在field里的序号
     * @param proxCode 当前term在field的序号
     */
    void writeProx(int termID, int proxCode) {
        if (payloadAttribute == null) {
            // 写入position数据, 将序号向左移动一位
            // 或然跟随规则(A, B?) :
            // 某个值A后面可能存在某个值B，也可能不存在，需要一个标志来表示后面是否跟随着B。
            //一般的情况下，在A后面放置一个Byte，为0则后面不存在B，为1则后面存在B，或者0则后面存在B，1则后面不存在B。
            //但这样要浪费一个Byte的空间，其实一个Bit就可以了。
            //在Lucene中，采取以下的方式：A的值左移一位，空出最后一位，作为标志位，来表示后面是否跟随B，所以在这种情况下，A/2是真正的A原来的值。
            writeVInt(1, proxCode << 1);
        } else {
            BytesRef payload = payloadAttribute.getPayload();
            if (payload != null && payload.length > 0) {
                // 左移一位,末位置为1,表示后面还跟着其他信息
                writeVInt(1, (proxCode << 1) | 1);
                writeVInt(1, payload.length);
                writeBytes(1, payload.bytes, payload.offset, payload.length);
                sawPayloads = true;
            } else {
                writeVInt(1, proxCode << 1);
            }
        }

        assert postingsArray == freqProxPostingsArray;
        freqProxPostingsArray.lastPositions[termID] = fieldState.position;
    }

    void writeOffsets(int termID, int offsetAccum) {
        final int startOffset = offsetAccum + offsetAttribute.startOffset();
        final int endOffset = offsetAccum + offsetAttribute.endOffset();
        assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
        // 存储当前term相对于上一个term的offset的差值, 比如上一个term是{125,131} ,当前term是 {132,139}, 那么存{7,7}, 这样能减少字节数,降低数据量
        // 第一个存储startOffset, 第二个存储length: 比如一个term的位置是:{125,131} 那么存储{125-116,6}, 116是前一个term的startOffset
        writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]);
        writeVInt(1, endOffset - startOffset);
        freqProxPostingsArray.lastOffsets[termID] = startOffset;
    }

    /**
     * 添加一个新的term
     * 写offset的前提是必须写prox, prox和offset成对出现,这样就能区分同一个term多次出现是如何拆分多次的prox和offset数据
     * 写入数据根据或然跟随原则来判断多少个字节是一个数据,prox占一个数据,offset占2个数据
     *
     * @param termID 当前term在field的值中的序号
     */
    @Override
    void newTerm(final int termID) {
        // First time we're seeing this term since the last
        // flush
        final FreqProxPostingsArray postings = freqProxPostingsArray;

        // docState.docID, 最近出现当前term的docID
        postings.lastDocIDs[termID] = docState.docID;
        if (!hasFreq) {
            assert postings.termFreqs == null;
            postings.lastDocCodes[termID] = docState.docID;
            fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
        } else {
            // 如果term是第一次出现,存docID*2
            postings.lastDocCodes[termID] = docState.docID << 1;
            //新的term默认freq是1,也就是第一次出现, 第一次出现不会写数据,因为当前doc还没处理完,不知道会不会再次遇到此term
            // 只有在处理下一个doc是再次遇到此term,才会写入freq信息
            postings.termFreqs[termID] = getTermFreq();
            if (hasProx) {
                // fieldState.position : 当前term的相对前一个term的位置增量
                writeProx(termID, fieldState.position);
                if (hasOffsets) {
                    // fieldState.offset: 默认值0
                    writeOffsets(termID, fieldState.offset);
                }
            } else {
                assert !hasOffsets;
            }
            fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
        }
        fieldState.uniqueTermCount++;
    }

    /**
     * 添加一个当前field里已经出现过的term
     * 写offset的前提是必须写prox, prox和offset成对出现,这样就能区分同一个term多次出现是如何拆分多次的prox和offset数据
     * 写入数据根据或然跟随原则来判断多少个字节是一个数据,prox占一个数据,offset占2个数据
     *
     * @param termID 当前term在field的值中最早出现的序号
     */
    @Override
    void addTerm(final int termID) {
        final FreqProxPostingsArray postings = freqProxPostingsArray;
        assert !hasFreq || postings.termFreqs[termID] > 0;

        // 如果不需要存储freq,也就是出现的频率
        if (!hasFreq) {
            assert postings.termFreqs == null;
            if (termFreqAtt.getTermFrequency() != 1) {
                throw new IllegalStateException("field \"" + fieldInfo.name + "\": must index term freq while using custom TermFrequencyAttribute");
            }
            // 之前的doc已经处理完了,否则的话可能是当前term在当前field里的第二次出现
            if (docState.docID != postings.lastDocIDs[termID]) {
                // New document; now encode docCode for previous doc:
                assert docState.docID > postings.lastDocIDs[termID];
                writeVInt(0, postings.lastDocCodes[termID]);
                postings.lastDocCodes[termID] = docState.docID - postings.lastDocIDs[termID];
                postings.lastDocIDs[termID] = docState.docID;
                fieldState.uniqueTermCount++;
            }
        }
        //也就是此term是当前field里第一次出现, 但在之前的doc的相同Field里出现过
        else if (docState.docID != postings.lastDocIDs[termID]) {
            assert docState.docID > postings.lastDocIDs[termID] : "id: " + docState.docID + " postings ID: " + postings.lastDocIDs[termID] + " termID: "
                + termID;
            // Term not yet seen in the current doc but previously
            // seen in other doc(s) since the last flush

            // Now that we know doc freq for previous doc, 现在我们可以正常处理此上一个doc里的此term了
            // 每个term至少是在第二次doc里出现才会写入freqs数据,因为在第一个doc里出现是还不知道此doc有没有解析完,还会不会再次出现
            // write it & lastDocCode
            // 如果当前term在当前doc是第一次出现,且在之前的doc里也只出现过一次, 也就是此term的freq还没有写入数据,
            // lastDocCodes正常是2*docID,将其末位置为1表示此term在此docID里出现一次, 因为只有一个doc,后续不会有数据,所以末位为1
            // 如果当前term在上一个doc里只出现了一次,末位+1的形式来表示,和出现多次的不一样, 此规则只适用于第一个doc
            // @see newTerm时将 postings.termFreqs[termID] 设置为1
            if (1 == postings.termFreqs[termID]) {
                writeVInt(0, postings.lastDocCodes[termID] | 1);
            }
            // 此term在之前的doc里不止出现一次
            else {
                // 记录此term出现的上一个docID, 实际是上一个docID相对于上上一个docID的差值 的2倍, 所以末位为0
                writeVInt(0, postings.lastDocCodes[termID]);
                // 记录此term在上一个doc里出现的次数
                writeVInt(0, postings.termFreqs[termID]);
            }

            // Init freq for the current document, 初始化当前doc的freq, 也就是1
            postings.termFreqs[termID] = getTermFreq();
            fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
            // 设置当前term两个doc之间的id差值
            postings.lastDocCodes[termID] = (docState.docID - postings.lastDocIDs[termID]) << 1;
            // 更新此term的最后出现docID, 也就是当前doc是此term的最后出现的doc了
            postings.lastDocIDs[termID] = docState.docID;
            if (hasProx) {
                writeProx(termID, fieldState.position);
                if (hasOffsets) {
                    postings.lastOffsets[termID] = 0;
                    writeOffsets(termID, fieldState.offset);
                }
            } else {
                assert !hasOffsets;
            }
            fieldState.uniqueTermCount++;
        }
        // 此term只在当前field里出现,且是第N(N>1)次出现
        else {
            // freq+1, 在处理当前doc时不知道某个term的freq,只有处理下一个doc时,才会写入这个doc的term的freq数据
            postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], getTermFreq());
            // 更新此term在此Field中的最大出现次数
            fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, postings.termFreqs[termID]);
            if (hasProx) {
                // 写位置时写入此term的当前position和上一次position的差值
                writeProx(termID, fieldState.position - postings.lastPositions[termID]);
                if (hasOffsets) {
                    writeOffsets(termID, fieldState.offset);
                }
            }
        }
    }

    private int getTermFreq() {
        int freq = termFreqAtt.getTermFrequency();
        if (freq != 1) {
            if (hasProx) {
                throw new IllegalStateException("field \"" + fieldInfo.name + "\": cannot index positions while using custom TermFrequencyAttribute");
            }
        }

        return freq;
    }

    @Override
    public void newPostingsArray() {
        freqProxPostingsArray = (FreqProxPostingsArray)postingsArray;
    }

    @Override
    ParallelPostingsArray createPostingsArray(int size) {
        IndexOptions indexOptions = fieldInfo.getIndexOptions();
        assert indexOptions != IndexOptions.NONE;
        boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
        boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        boolean hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
    }

    static final class FreqProxPostingsArray extends ParallelPostingsArray {

        public FreqProxPostingsArray(int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
            super(size);
            if (writeFreqs) {
                termFreqs = new int[size];
            }
            lastDocIDs = new int[size];
            lastDocCodes = new int[size];
            if (writeProx) {
                lastPositions = new int[size];
                if (writeOffsets) {
                    lastOffsets = new int[size];
                }
            } else {
                assert !writeOffsets;
            }
            System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" + writeOffsets);
        }

        /**
         * 当前term在当前Field里出现的次数
         */
        int termFreqs[];                                   // # times this term occurs in the current doc
        /**
         * 当前term最后出现的doc的ID
         */
        int lastDocIDs[];                                  // Last docID where this term occurred
        /**
         * lastDocCodes[3] = 2; 表示第3个term的最新的docID(比如15)比上一个出现此term(比如13)的docID 大 2
         */
        int lastDocCodes[];                                // Code for prior doc
        /**
         * 上次处理完的此词的位置
         */
        int lastPositions[];                               // Last position where this term occurred
        int lastOffsets[];                                 // Last endOffset where this term occurred

        @Override
        ParallelPostingsArray newInstance(int size) {
            return new FreqProxPostingsArray(size, termFreqs != null, lastPositions != null, lastOffsets != null);
        }

        @Override
        void copyTo(ParallelPostingsArray toArray, int numToCopy) {
            assert toArray instanceof FreqProxPostingsArray;
            FreqProxPostingsArray to = (FreqProxPostingsArray)toArray;

            super.copyTo(toArray, numToCopy);

            System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
            System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
            if (lastPositions != null) {
                assert to.lastPositions != null;
                System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
            }
            if (lastOffsets != null) {
                assert to.lastOffsets != null;
                System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
            }
            if (termFreqs != null) {
                assert to.termFreqs != null;
                System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
            }
        }

        @Override
        int bytesPerPosting() {
            int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES;
            if (lastPositions != null) {
                bytes += Integer.BYTES;
            }
            if (lastOffsets != null) {
                bytes += Integer.BYTES;
            }
            if (termFreqs != null) {
                bytes += Integer.BYTES;
            }

            return bytes;
        }
    }
}
