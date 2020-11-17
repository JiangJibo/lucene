package org.apache.lucene.jiangjibo;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wb-jjb318191
 * @create 2020-09-27 11:10
 */
public class TermInSetQueryTest {

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
    public void testTermsQuery() throws IOException {
        long time1 = System.currentTimeMillis();
        TermInSetQuery query = new TermInSetQuery("city",
            buildValuesArray("杭州市", "南京市", "北京市", "西安市", "合肥市", "上海市", "苏州市", "无锡市"));
        TopDocs topDocs = isearcher.search(query, 10);
        System.out.println("totalHits:" + topDocs.totalHits);
        System.out.println("maxScore:" + topDocs.getMaxScore());
        System.out.println(System.currentTimeMillis() - time1);
    }

    private List<BytesRef> buildValuesArray(String... values) {
        return Arrays.stream(values).map(s -> new BytesRef(s)).collect(Collectors.toList());
    }

}
