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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;

final class FreqProxTermsWriter extends TermsHash {

    public FreqProxTermsWriter(DocumentsWriterPerThread docWriter, TermsHash termVectors) {
        super(docWriter, true, termVectors);
    }

    /**
     * 在刷盘是应用Term Delete
     *
     * @param state
     * @param fields
     * @throws IOException
     */
    private void applyDeletes(SegmentWriteState state, Fields fields) throws IOException {
        // Process any pending Term deletes for this newly
        // flushed segment:
        if (state.segUpdates != null && state.segUpdates.deleteTerms.size() > 0) {
            Map<Term, Integer> segDeletes = state.segUpdates.deleteTerms;
            List<Term> deleteTerms = new ArrayList<>(segDeletes.keySet());
            // 对term排序, 按term所属的Field或者字节排序
            Collections.sort(deleteTerms);
            String lastField = null;
            TermsEnum termsEnum = null;
            PostingsEnum postingsEnum = null;
            for (Term deleteTerm : deleteTerms) {
                if (deleteTerm.field().equals(lastField) == false) {
                    lastField = deleteTerm.field();
                    Terms terms = fields.terms(lastField);
                    if (terms != null) {
                        // Term遍历器
                        termsEnum = terms.iterator();
                    } else {
                        termsEnum = null;
                    }
                }

                // 是否有这个term
                if (termsEnum != null && termsEnum.seekExact(deleteTerm.bytes())) {
                    postingsEnum = termsEnum.postings(postingsEnum, 0);
                    // term删除上限,也就是在删除term是内存里总共有多少个doc, 不能把之后的doc删掉
                    int delDocLimit = segDeletes.get(deleteTerm);
                    assert delDocLimit < PostingsEnum.NO_MORE_DOCS;
                    while (true) {
                        int doc = postingsEnum.nextDoc();
                        // 如果符合删除
                        if (doc < delDocLimit) {
                            if (state.liveDocs == null) {
                                state.liveDocs = state.segmentInfo.getCodec().liveDocsFormat().newLiveDocs(state.segmentInfo.maxDoc());
                            }
                            // 将liveDocs里移除被删除的doc
                            if (state.liveDocs.get(doc)) {
                                state.delCountOnFlush++;
                                state.liveDocs.clear(doc);
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    public void flush(Map<String, TermsHashPerField> fieldsToFlush, final SegmentWriteState state,
        Sorter.DocMap sortMap) throws IOException {
        super.flush(fieldsToFlush, state, sortMap);

        // Gather all fields that saw any postings:
        List<FreqProxTermsWriterPerField> allFields = new ArrayList<>();

        for (TermsHashPerField f : fieldsToFlush.values()) {
            final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField)f;
            if (perField.bytesHash.size() > 0) {
                perField.sortPostings();
                assert perField.fieldInfo.getIndexOptions() != IndexOptions.NONE;
                allFields.add(perField);
            }
        }

        // Sort by field name, 按field名称排序
        CollectionUtil.introSort(allFields);

        Fields fields = new FreqProxFields(allFields);
        // 应用Term Delete 因为在刷盘时已经处理了term删除，所以这里可以将其清除
        applyDeletes(state, fields);
        if (sortMap != null) {
            fields = new SortingLeafReader.SortingFields(fields, state.fieldInfos, sortMap);
        }

        // Lucene50PostingsFormat#fieldsConsumer = BlockTreeTermsWriter
        FieldsConsumer consumer = state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state);
        boolean success = false;
        try {
            consumer.write(fields);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(consumer);
            } else {
                IOUtils.closeWhileHandlingException(consumer);
            }
        }

    }

    @Override
    public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
        return new FreqProxTermsWriterPerField(invertState, this, fieldInfo,
            nextTermsHash.addField(invertState, fieldInfo));
    }
}
