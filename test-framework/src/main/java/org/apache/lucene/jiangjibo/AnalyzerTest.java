package org.apache.lucene.jiangjibo;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.junit.Test;

/**
 * @author wb-jjb318191
 * @create 2020-09-09 10:36
 */
public class AnalyzerTest {

    @Test
    public void testAnalyzerInvoking() throws IOException {

        /*Tokenizer tokenizer = new WhitespaceTokenizer();

        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(tokenizer);
            }
        };

        //analyzer = new StandardAnalyzer();

        TokenStream tokenStream = analyzer.tokenStream("text", new StringReader("中国 浙江 杭州 西湖"));
        //tokenStream = new LowerCaseFilter(tokenStream);
        tokenStream.reset();
        TypeAttribute typeAttr = tokenStream.getAttribute(TypeAttribute.class);
        OffsetAttribute offsetAttr = tokenStream.getAttribute(OffsetAttribute.class);
        CharTermAttribute charTermAttr = tokenStream.getAttribute(CharTermAttribute.class);
        PositionIncrementAttribute posIncrAttr = tokenStream.getAttribute(PositionIncrementAttribute.class);

        int position = 0;
        while (tokenStream.incrementToken()) {
            int positionIncrement = posIncrAttr.getPositionIncrement();
            if (positionIncrement > 0) {
                position += positionIncrement;
                System.out.println();
                System.out.print(position + " : ");
            }

            System.out.printf("[%s : %d ->  %d : %s]", charTermAttr.toString(), offsetAttr.startOffset(), offsetAttr.endOffset(),
                typeAttr.type());
        }*/
    }

}
