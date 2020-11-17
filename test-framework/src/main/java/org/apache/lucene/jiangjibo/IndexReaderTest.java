package org.apache.lucene.jiangjibo;

import java.io.IOException;
import java.nio.file.Paths;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wb-jjb318191
 * @create 2020-10-13 15:03
 */
public class IndexReaderTest {

    private DirectoryReader indexReader;

    @Before
    public void init() throws IOException {
        Directory directory = FSDirectory.open(Paths.get("D:\\lucene-data"));
        indexReader = DirectoryReader.open(directory);
    }

    @Test
    public void visitFields() throws IOException {
        Document document = indexReader.document(100000, ImmutableSet.of("country", "province"));
        System.out.println(document.toString());
    }

}
