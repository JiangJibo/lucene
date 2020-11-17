package org.apache.lucene.jiangjibo;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wb-jjb318191
 * @create 2020-09-09 19:02
 */
public class Ipv4IndexSearcherTest {

    private IndexSearcher isearcher;

    @Before
    public void init() throws IOException {
        Directory directory = FSDirectory.open(Paths.get("D:\\lucene-data"));
        DirectoryReader ireader = DirectoryReader.open(directory);
        isearcher = new IndexSearcher(ireader);
    }

    @After
    public void after() throws IOException {
        isearcher.getIndexReader().close();
    }

    @Test
    public void searchIpv4DataTest() throws IOException {
        Query query = new TermQuery(new Term("address", "江苏省"));
        ScoreDoc[] hits = isearcher.search(query, 4).scoreDocs;
        // Iterate through the results:
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println(hits[i].doc + ":\t" + hitDoc.get("address"));
        }
    }

    @Test
    public void searchWithSort() throws IOException {

        Sort sort = Sort.INDEXORDER;
        sort = Sort.RELEVANCE;
        sort = new Sort(new SortField("start", Type.LONG));

        Query query = new TermQuery(new Term("city", new BytesRef("杭州市")));
        ScoreDoc[] hits = isearcher.search(query, 4, Sort.INDEXORDER).scoreDocs ;
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println(hitDoc.get("start") + ":\t" + hitDoc.get("address"));
        }
    }

    @Test
    public void testSearchWithCollector() throws IOException {
        Query query = new TermQuery(new Term("address", new BytesRef("江苏省")));
        CachingCollector collector = CachingCollector.create(true, 100);
        isearcher.search(query, collector);

        final float[] maxScore = {0};
        Collector other = new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) {
                return new LeafCollector() {

                    Scorer scorer;

                    @Override
                    public void setScorer(Scorer scorer) {
                        this.scorer = scorer;
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        float score = scorer.score();
                        if (maxScore[0] < score) {
                            System.out.println(String.format("docID:%d, score:%s", doc, scorer.score() + ""));
                        }
                        maxScore[0] = Math.max(maxScore[0], score);
                    }
                };
            }

            @Override
            public boolean needsScores() {
                return true;
            }
        };
        collector.replay(other);

    }

    @Test
    public void searchWithDocValues() throws IOException {
        Query rangeQuery = SortedNumericDocValuesField.newSlowRangeQuery("start", 100, 1000);
        ScoreDoc[] hits = isearcher.search(rangeQuery, 3).scoreDocs;
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println(hitDoc.get("start") + ":\t" + hitDoc.get("address"));
        }
    }



}
