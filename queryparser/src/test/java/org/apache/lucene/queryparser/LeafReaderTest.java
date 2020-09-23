package org.apache.lucene.queryparser;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wb-jjb318191
 * @create 2020-09-23 18:52
 */
public class LeafReaderTest {

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
    public void getLeafReader() throws IOException {
        LeafReader leafReader = isearcher.getIndexReader().leaves().get(0).reader();
        TermsEnum termsEnum = leafReader.terms("city").iterator();
        boolean exact = termsEnum.seekExact(new BytesRef("杭州市"));
        PostingsEnum postingsEnum = null;
        if (!exact) {
            return;
        }
        System.out.println("docFreq:" + termsEnum.docFreq());
        System.out.println("totalTermFreq:" + termsEnum.totalTermFreq());
        PostingsEnum postings = termsEnum.postings(postingsEnum, PostingsEnum.ALL);
        while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            System.out.println(String.format("docID:%d, freq:%d, position:%d",
                postings.docID(), postings.freq(), postings.nextPosition()));
        }
    }

}
