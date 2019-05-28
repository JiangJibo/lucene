package org.apache.lucene.queryparser.bob.synonym;

import java.io.IOException;

/**
 * @author wb-jjb318191
 * @create 2019-05-28 9:53
 */
public interface SynonymEngine {

    String[] getSynonyms(String s) throws IOException;
}