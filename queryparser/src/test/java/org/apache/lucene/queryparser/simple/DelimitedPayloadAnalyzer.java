package org.apache.lucene.queryparser.simple;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * DelimitedPayload的分词器
 *
 * @author wb-jjb318191
 * @create 2019-05-27 17:42
 */
public class DelimitedPayloadAnalyzer extends Analyzer {

    private Analyzer internalAnalyzer;

    public DelimitedPayloadAnalyzer(Analyzer internalAnalyzer) {
        this.internalAnalyzer = internalAnalyzer;
    }



    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        StandardTokenizer source = new StandardTokenizer();
        return new TokenStreamComponents(source, new SynonymFilter(new StopFilter(new LowerCaseFilter(source),
            new CharArraySet(StopAnalyzer.ENGLISH_STOP_WORDS_SET, true)), new TestSynonymEngine()));
    }
}
