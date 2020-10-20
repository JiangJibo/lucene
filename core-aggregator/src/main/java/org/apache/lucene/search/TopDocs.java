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
package org.apache.lucene.search;

import org.apache.lucene.util.PriorityQueue;

/**
 * Represents hits returned by {@link
 * IndexSearcher#search(Query, int)}.
 */
public class TopDocs {

    /**
     * The total number of hits for the query.
     */
    public long totalHits;

    /**
     * The top hits for the query. doc按分数由高到低排序
     */
    public ScoreDoc[] scoreDocs;

    /**
     * Stores the maximum score value encountered, needed for normalizing.
     */
    private float maxScore;

    /**
     * Returns the maximum score value encountered. Note that in case
     * scores are not tracked, this returns {@link Float#NaN}.
     */
    public float getMaxScore() {
        return maxScore;
    }

    /**
     * Sets the maximum score value encountered.
     */
    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    /**
     * Constructs a TopDocs with a default maxScore=Float.NaN.
     */
    TopDocs(long totalHits, ScoreDoc[] scoreDocs) {
        this(totalHits, scoreDocs, Float.NaN);
    }

    public TopDocs(long totalHits, ScoreDoc[] scoreDocs, float maxScore) {
        this.totalHits = totalHits;
        this.scoreDocs = scoreDocs;
        this.maxScore = maxScore;
    }

    // Refers to one hit:
    private final static class ShardRef {

        // Which shard (index into shardHits[]):
        final int shardIndex;

        // True if we should use the incoming ScoreDoc.shardIndex for sort order
        final boolean useScoreDocIndex;

        // Which hit within the shard:
        int hitIndex;

        ShardRef(int shardIndex, boolean useScoreDocIndex) {
            this.shardIndex = shardIndex;
            this.useScoreDocIndex = useScoreDocIndex;
        }

        @Override
        public String toString() {
            return "ShardRef(shardIndex=" + shardIndex + " hitIndex=" + hitIndex + ")";
        }

        int getShardIndex(ScoreDoc scoreDoc) {
            if (useScoreDocIndex) {
                if (scoreDoc.shardIndex == -1) {
                    throw new IllegalArgumentException("setShardIndex is false but TopDocs[" + shardIndex + "].scoreDocs[" + hitIndex + "] is not set");
                }
                return scoreDoc.shardIndex;
            } else {
                // NOTE: we don't assert that shardIndex is -1 here, because caller could in fact have set it but asked us to ignore it now
                return shardIndex;
            }
        }
    }

    /**
     * if we need to tie-break since score / sort value are the same we first compare shard index (lower shard wins)
     * and then iff shard index is the same we use the hit index.
     */
    static boolean tieBreakLessThan(ShardRef first, ScoreDoc firstDoc, ShardRef second, ScoreDoc secondDoc) {
        final int firstShardIndex = first.getShardIndex(firstDoc);
        final int secondShardIndex = second.getShardIndex(secondDoc);
        // Tie break: earlier shard wins
        if (firstShardIndex < secondShardIndex) {
            return true;
        } else if (firstShardIndex > secondShardIndex) {
            return false;
        } else {
            // Tie break in same shard: resolve however the
            // shard had resolved it:
            assert first.hitIndex != second.hitIndex;
            return first.hitIndex < second.hitIndex;
        }
    }

    // Specialized MergeSortQueue that just merges by
    // relevance score, descending:
    private static class ScoreMergeSortQueue extends PriorityQueue<ShardRef> {

        final ScoreDoc[][] shardHits;

        public ScoreMergeSortQueue(TopDocs[] shardHits) {
            super(shardHits.length);
            this.shardHits = new ScoreDoc[shardHits.length][];
            for (int shardIDX = 0; shardIDX < shardHits.length; shardIDX++) {
                this.shardHits[shardIDX] = shardHits[shardIDX].scoreDocs;
            }
        }

        /**
         * 计算两个分片的命中集合下，当前序号的元素的分值大小
         *
         * @param first
         * @param second
         * @return
         */
        // Returns true if first is < second
        @Override
        public boolean lessThan(ShardRef first, ShardRef second) {
            assert first != second;
            ScoreDoc firstScoreDoc = shardHits[first.shardIndex][first.hitIndex];
            ScoreDoc secondScoreDoc = shardHits[second.shardIndex][second.hitIndex];
            if (firstScoreDoc.score < secondScoreDoc.score) {
                return false;
            } else if (firstScoreDoc.score > secondScoreDoc.score) {
                return true;
            } else {
                // 如果分值相等
                return tieBreakLessThan(first, firstScoreDoc, second, secondScoreDoc);
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static class MergeSortQueue extends PriorityQueue<ShardRef> {

        // These are really FieldDoc instances:
        final ScoreDoc[][] shardHits;
        final FieldComparator<?>[] comparators;
        final int[] reverseMul;

        public MergeSortQueue(Sort sort, TopDocs[] shardHits) {
            super(shardHits.length);
            this.shardHits = new ScoreDoc[shardHits.length][];
            for (int shardIDX = 0; shardIDX < shardHits.length; shardIDX++) {
                final ScoreDoc[] shard = shardHits[shardIDX].scoreDocs;
                //System.out.println("  init shardIdx=" + shardIDX + " hits=" + shard);
                if (shard != null) {
                    this.shardHits[shardIDX] = shard;
                    // Fail gracefully if API is misused:
                    for (int hitIDX = 0; hitIDX < shard.length; hitIDX++) {
                        final ScoreDoc sd = shard[hitIDX];
                        if (!(sd instanceof FieldDoc)) {
                            throw new IllegalArgumentException(
                                "shard " + shardIDX + " was not sorted by the provided Sort (expected FieldDoc but got ScoreDoc)");
                        }
                        final FieldDoc fd = (FieldDoc)sd;
                        if (fd.fields == null) {
                            throw new IllegalArgumentException("shard " + shardIDX
                                + " did not set sort field values (FieldDoc.fields is null); you must pass fillFields=true to IndexSearcher.search on each "
                                + "shard");
                        }
                    }
                }
            }

            final SortField[] sortFields = sort.getSort();
            comparators = new FieldComparator[sortFields.length];
            reverseMul = new int[sortFields.length];
            for (int compIDX = 0; compIDX < sortFields.length; compIDX++) {
                final SortField sortField = sortFields[compIDX];
                comparators[compIDX] = sortField.getComparator(1, compIDX);
                reverseMul[compIDX] = sortField.getReverse() ? -1 : 1;
            }
        }

        // Returns true if first is < second
        @Override
        public boolean lessThan(ShardRef first, ShardRef second) {
            assert first != second;
            final FieldDoc firstFD = (FieldDoc)shardHits[first.shardIndex][first.hitIndex];
            final FieldDoc secondFD = (FieldDoc)shardHits[second.shardIndex][second.hitIndex];
            //System.out.println("  lessThan:\n     first=" + first + " doc=" + firstFD.doc + " score=" + firstFD.score + "\n    second=" + second + " doc="
            // + secondFD.doc + " score=" + secondFD.score);

            for (int compIDX = 0; compIDX < comparators.length; compIDX++) {
                final FieldComparator comp = comparators[compIDX];
                //System.out.println("    cmp idx=" + compIDX + " cmp1=" + firstFD.fields[compIDX] + " cmp2=" + secondFD.fields[compIDX] + " reverse=" +
                // reverseMul[compIDX]);

                final int cmp = reverseMul[compIDX] * comp.compareValues(firstFD.fields[compIDX], secondFD.fields[compIDX]);

                if (cmp != 0) {
                    //System.out.println("    return " + (cmp < 0));
                    return cmp < 0;
                }
            }
            return tieBreakLessThan(first, firstFD, second, secondFD);
        }
    }

    /**
     * Returns a new TopDocs, containing topN results across
     * the provided TopDocs, sorting by score. Each {@link TopDocs}
     * instance must be sorted.
     *
     * @lucene.experimental
     * @see #merge(int, int, TopDocs[], boolean)
     */
    public static TopDocs merge(int topN, TopDocs[] shardHits) {
        return merge(0, topN, shardHits, true);
    }

    /**
     * Same as {@link #merge(int, TopDocs[])} but also ignores the top
     * {@code start} top docs. This is typically useful for pagination.
     *
     * Note: If {@code setShardIndex} is true, this method will assume the incoming order of {@code shardHits} reflects
     * each shard's index and will fill the {@link ScoreDoc#shardIndex}, otherwise
     * it must already be set for all incoming {@code ScoreDoc}s, which can be useful when doing multiple reductions
     * (merges) of TopDocs.
     *
     * @lucene.experimental
     */
    public static TopDocs merge(int start, int topN, TopDocs[] shardHits, boolean setShardIndex) {
        return mergeAux(null, start, topN, shardHits, setShardIndex);
    }

    /**
     * Returns a new TopFieldDocs, containing topN results across
     * the provided TopFieldDocs, sorting by the specified {@link
     * Sort}.  Each of the TopDocs must have been sorted by
     * the same Sort, and sort field values must have been
     * filled (ie, <code>fillFields=true</code> must be
     * passed to {@link TopFieldCollector#create}).
     *
     * @lucene.experimental
     * @see #merge(Sort, int, int, TopFieldDocs[], boolean)
     */
    public static TopFieldDocs merge(Sort sort, int topN, TopFieldDocs[] shardHits) {
        return merge(sort, 0, topN, shardHits, true);
    }

    /**
     * Same as {@link #merge(Sort, int, TopFieldDocs[])} but also ignores the top
     * {@code start} top docs. This is typically useful for pagination.
     *
     * Note: If {@code setShardIndex} is true, this method will assume the incoming order of {@code shardHits} reflects
     * each shard's index and will fill the {@link ScoreDoc#shardIndex}, otherwise
     * it must already be set for all incoming {@code ScoreDoc}s, which can be useful when doing multiple reductions
     * (merges) of TopDocs.
     *
     * @lucene.experimental
     */
    public static TopFieldDocs merge(Sort sort, int start, int topN, TopFieldDocs[] shardHits, boolean setShardIndex) {
        if (sort == null) {
            throw new IllegalArgumentException("sort must be non-null when merging field-docs");
        }
        return (TopFieldDocs)mergeAux(sort, start, topN, shardHits, setShardIndex);
    }

    /**
     * Auxiliary method used by the {@link #merge} impls. A sort value of null
     * is used to indicate that docs should be sorted by score.
     *
     * @param sort
     * @param start
     * @param size
     * @param shardHits     每个分片的命中数据
     * @param setShardIndex
     * @return
     */
    private static TopDocs mergeAux(Sort sort, int start, int size, TopDocs[] shardHits, boolean setShardIndex) {

        final PriorityQueue<ShardRef> queue;
        if (sort == null) {
            // 按分数排序
            queue = new ScoreMergeSortQueue(shardHits);
        } else {
            // 按Field排序
            queue = new MergeSortQueue(sort, shardHits);
        }

        long totalHitCount = 0;
        int availHitCount = 0;
        float maxScore = Float.MIN_VALUE;
        for (int shardIDX = 0; shardIDX < shardHits.length; shardIDX++) {
            final TopDocs shard = shardHits[shardIDX];
            // totalHits can be non-zero even if no hits were
            // collected, when searchAfter was used:
            totalHitCount += shard.totalHits;
            if (shard.scoreDocs != null && shard.scoreDocs.length > 0) {
                // 统计分片hit总数
                availHitCount += shard.scoreDocs.length;
                // 在添加shared的时候回调整其在queue中的位置
                queue.add(new ShardRef(shardIDX, setShardIndex == false));
                // 统计多个分片的总的最高分
                maxScore = Math.max(maxScore, shard.getMaxScore());
            }
        }

        if (availHitCount == 0) {
            maxScore = Float.NaN;
        }

        final ScoreDoc[] hits;
        if (availHitCount <= start) {
            hits = new ScoreDoc[0];
        } else {
            // 从所有的分片中统计出topN
            hits = new ScoreDoc[Math.min(size, availHitCount - start)];
            int requestedResultWindow = start + size;
            // 需要提取的数据量, 如果是分页的话，那么是 start + size
            int numIterOnHits = Math.min(availHitCount, requestedResultWindow);
            int hitUpto = 0;
            // 遍历N次获取topN
            while (hitUpto < numIterOnHits) {
                assert queue.size() > 0;
                ShardRef ref = queue.top();
                // 获取指定分片的hitIndex序号的DOC
                final ScoreDoc hit = shardHits[ref.shardIndex].scoreDocs[ref.hitIndex++];
                if (setShardIndex) {
                    // caller asked us to record shardIndex (index of the TopDocs array) this hit is coming from:
                    hit.shardIndex = ref.shardIndex;
                } else if (hit.shardIndex == -1) {
                    throw new IllegalArgumentException(
                        "setShardIndex is false but TopDocs[" + ref.shardIndex + "].scoreDocs[" + (ref.hitIndex - 1) + "] is not set");
                }

                if (hitUpto >= start) {
                    hits[hitUpto - start] = hit;
                }

                hitUpto++;

                // 每提取一个元素后，都会按照每个节点的当前最大的分数来更新其在queue里的顺序
                if (ref.hitIndex < shardHits[ref.shardIndex].scoreDocs.length) {
                    // Not done with this these TopDocs yet:  更新节点顺序
                    queue.updateTop();
                } else {
                    queue.pop();
                }
            }
        }

        if (sort == null) {
            return new TopDocs(totalHitCount, hits, maxScore);
        } else {
            return new TopFieldDocs(totalHitCount, hits, sort.getSort(), maxScore);
        }
    }
}
