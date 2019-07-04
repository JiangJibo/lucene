package org.apache.lucene.queryparser.bob;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Lucene使用方法
 *
 * @author wb-jjb318191
 * @create 2019-05-08 15:23
 */
public class LuceneUsageExample {

    private Directory dictionary;

    @Before
    public void init() throws IOException {
        dictionary = new SimpleFSDirectory(Paths.get("D:\\lucene-temp"));
    }

    @Test
    public void testUseLucene() throws IOException, ParseException {
        Analyzer analyzer = new StandardAnalyzer();

        // Store the index in memory:
        Directory directory = new RAMDirectory();
        // To store an index on disk, use this instead:
        //Directory directory = FSDirectory.open("/tmp/testindex");
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter iwriter = new IndexWriter(directory, config);
        Document doc = new Document();
        String text = "This is the text to be indexed.";
        doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
        iwriter.addDocument(doc);
        iwriter.close();

        // Now search the index:
        DirectoryReader ireader = DirectoryReader.open(directory);
        IndexSearcher isearcher = new IndexSearcher(ireader);
        // Parse a simple query that searches for "text":
        QueryParser parser = new QueryParser("fieldname", analyzer);
        Query query = parser.parse("text");
        ScoreDoc[] hits = isearcher.search(query, 1, new Sort()).scoreDocs;
        assertEquals(1, hits.length);
        // Iterate through the results:
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println(hitDoc.get("fieldname"));
            assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
        }
        ireader.close();
        directory.close();
    }

    @Test
    public void testInsertDocument() throws IOException {

        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setUseCompoundFile(false);

        IndexWriter writer = new IndexWriter(dictionary, iwc);

        // field必须具备索引或者存储中的一个特性,如果两个都不要,那么这个属性就没用了
        FieldType ft1 = new FieldType();
        ft1.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        ft1.setStored(true);

        FieldType ft2 = new FieldType();
        ft2.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        ft2.setStored(true);
        ft2.setDocValuesType(DocValuesType.NUMERIC);

        Document doc1 = new Document();
        Field field = new Field("title", "java introduction asda", ft1);
        doc1.add(field);
        doc1.add(new Field("content", "java python works java well sdfasdf", ft1));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new Field("content", "java is the best language", ft1));
        Field f2 = new SortedNumericDocValuesField("number", 12);
        doc2.add(f2);

        writer.addDocument(doc2);

        Document doc3 = new Document();
        doc3.add(new Field("content", "java", ft1));
        writer.addDocument(doc3);

        //writer.optimize();
        writer.close();
    }

    @Test
    public void testReadDocument() throws IOException {
        // BaseCompositeReader, StandardDirectoryReader
        IndexReader indexReader = DirectoryReader.open(dictionary);
        Document document = indexReader.document(1);
        System.out.println(document.getField("content"));
    }

    /**
     * 测试Term Query
     */
    @Test
    public void testSearch() throws IOException {
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(dictionary));
        TermQuery termQuery = new TermQuery(new Term("content", "java"));
        TopDocs topDocs = indexSearcher.search(termQuery, 2);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document document = indexSearcher.doc(scoreDoc.doc);
            System.out.println(document.toString());
        }
    }

}
