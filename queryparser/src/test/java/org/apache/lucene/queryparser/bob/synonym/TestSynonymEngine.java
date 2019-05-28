package org.apache.lucene.queryparser.bob.synonym;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wb-jjb318191
 * @create 2019-05-28 9:53
 */
public class TestSynonymEngine implements SynonymEngine {

    public static final Map<String, String[]> map = new HashMap<>();

    static {
        map.put("quick", new String[]{"fast", "speedy"});
    }

    @Override
    public String[] getSynonyms(String s) throws IOException {
        return map.get(s);
    }
}
