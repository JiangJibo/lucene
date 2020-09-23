package org.apache.lucene.queryparser;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Before;
import org.junit.Test;

/**
 * DocValues测试
 *
 * @author wb-jjb318191
 * @create 2020-09-21 15:32
 */
public class DocValuesTest {

    private IndexWriter indexWriter;

    private IndexSearcher indexSearcher;

    @Before
    public void init() throws IOException {
        // Store the index in memory:
        // To store an index on disk, use this instead:
        Directory directory = FSDirectory.open(Paths.get("D:\\lucene-docValues-data"));
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        indexWriter = new IndexWriter(directory, config);

        DirectoryReader ireader = DirectoryReader.open(directory);
        indexSearcher = new IndexSearcher(ireader);
    }

    @Test
    public void testWriteWithDocValues() throws IOException {
        for (int i = 0; i < 10; i++) {
            Document document = new Document();
            document.add(new NumericDocValuesField("age", i));
            indexWriter.addDocument(document);
        }
        indexWriter.flush();
        // close时会触发一次merge
        indexWriter.close();
    }

    @Test
    public void searchWithDocValues() throws IOException {
        Query query = SortedNumericDocValuesField.newSlowRangeQuery("age", 45L, 100L);
        TopDocs topDocs = indexSearcher.search(query, 3);
        System.out.println("总计命中: " + topDocs.totalHits);
        Arrays.stream(topDocs.scoreDocs).forEach(scoreDoc -> System.out.println(scoreDoc.doc));
    }

    @Test
    public void searchWithSort() throws IOException {
        Query query = NumericDocValuesField.newSlowRangeQuery("age", 1, 10);
        Sort sort = new Sort(new SortField("age", Type.LONG, true));

        TopDocs topDocs = indexSearcher.search(query, 2, sort);
        System.out.println("总计命中: " + topDocs.totalHits);
        Arrays.stream(topDocs.scoreDocs).forEach(scoreDoc -> System.out.println(scoreDoc.doc));
    }

    @Test
    public void sortedNumericDocValues() throws IOException {
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("age", 50));
        document.add(new SortedNumericDocValuesField("age", 35));
        document.add(new SortedNumericDocValuesField("age", 48));
        document.add(new SortedNumericDocValuesField("age", 21));
        indexWriter.addDocument(document);
        indexWriter.flush();
        // close时会触发一次merge
        indexWriter.close();
    }

    @Test
    public void searchDocValues() throws IOException {
        LeafReader leafReader = indexSearcher.getIndexReader().leaves().get(0).reader();
        SortedNumericDocValues docValues = leafReader.getSortedNumericDocValues("age");

        int advance = docValues.advance(2);

        System.out.println(docValues.advanceExact(advance));

        int docId;
        while ((docId = docValues.nextDoc()) != Integer.MAX_VALUE) {
            System.out.println(String.format("docId: %d, longValue: %d", docId, docValues.nextValue()));
        }
    }
}
