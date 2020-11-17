package org.apache.lucene.jiangjibo;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

/**
 * @author JiangJibo
 * @create 2019-12-19 22:16
 */
public class IndexSearcherTest {

    @Test
    public void testUseLucene() throws IOException, ParseException {
        Analyzer analyzer = new StandardAnalyzer();

        // Store the index in memory:
        //Directory directory = new RAMDirectory();
        // To store an index on disk, use this instead:
        Directory directory = FSDirectory.open(Paths.get("D:\\lucene-data"));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter iwriter = new IndexWriter(directory, config);
        Document doc1 = new Document();
        doc1.add(new Field("fieldname", "This is the text to be indexed xxx.", TextField.TYPE_STORED));
        iwriter.addDocument(doc1);
        Document doc2 = new Document();
        doc2.add(new Field("fieldname", "This is the text to be text indexed.", TextField.TYPE_STORED));
        iwriter.addDocument(doc2);
        Document doc3 = new Document();
        doc3.add(new Field("fieldname", "This is the text sss aaa.", TextField.TYPE_STORED));
        iwriter.addDocument(doc3);
        Document doc4 = new Document();
        doc4.add(new Field("fieldname", "aaa xxxx text sdfs.", TextField.TYPE_STORED));
        iwriter.addDocument(doc4);
        Document doc5 = new Document();
        doc5.add(new Field("fieldname", "text sdfs.", TextField.TYPE_STORED));
        iwriter.addDocument(doc5);
        iwriter.close();

        // Now search the index:
        DirectoryReader ireader = DirectoryReader.open(directory);
        IndexSearcher isearcher = new IndexSearcher(ireader);
        // Parse a simple query that searches for "text":
        QueryParser parser = new QueryParser("fieldname", analyzer);
        Query query = parser.parse("text");
        ScoreDoc[] hits = isearcher.search(query, 4).scoreDocs;
        // Iterate through the results:
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println(hitDoc.get("fieldname"));
        }
        ireader.close();
        directory.close();
    }

}
