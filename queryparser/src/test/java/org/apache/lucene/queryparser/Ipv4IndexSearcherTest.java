package org.apache.lucene.queryparser;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
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
        Query query = new TermQuery(new Term("address", new BytesRef("江苏省")));
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

        Query query = new TermQuery(new Term("address", new BytesRef("江苏省")));
        ScoreDoc[] hits = isearcher.search(query, 4, Sort.INDEXORDER).scoreDocs;
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println(hitDoc.get("start") + ":\t" + hitDoc.get("address"));
        }
    }

}
