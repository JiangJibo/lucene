package org.apache.lucene.queryparser;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Before;
import org.junit.Test;

/**
 * @author JiangJibo
 * @create 2020-10-02 17:30
 */
public class EarlyTerminateCollectorTest {

    private IndexSearcher indexSearcher;

    @Before
    public void init() throws IOException {
        Directory directory = FSDirectory.open(Paths.get("D:\\lucene-data"));
        DirectoryReader ireader = DirectoryReader.open(directory);
        indexSearcher = new IndexSearcher(ireader);
    }

    @Test
    public void testEarlyTerminateCollector() throws IOException {

        TermQuery province = new TermQuery(new Term("province", "浙江省"));
        TermQuery city = new TermQuery(new Term("city", "杭州市"));
        TermQuery region = new TermQuery(new Term("region", "西湖区"));
        BooleanQuery booleanQuery = new BooleanQuery.Builder()
            .add(city, Occur.FILTER)
            .add(province, Occur.MUST)
            .add(region, Occur.MUST)
            .build();


        Sort sort = new Sort(new SortField("start", Type.LONG));
        TopFieldCollector collector = TopFieldCollector.create(sort, 10, true, false, false, false);

        indexSearcher.search(booleanQuery, collector);

        TopFieldDocs topFieldDocs = collector.topDocs();

        System.out.println("总共命中: " + topFieldDocs.totalHits);

        for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
            System.out.println("docID:" + scoreDoc.doc + ", score:" + scoreDoc.score + ", address:" + indexSearcher.doc(scoreDoc.doc).get("address"));
        }


    }

}
