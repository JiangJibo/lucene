package org.apache.lucene.queryparser.bob.synonym;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.junit.Test;

/**
 * @author wb-jjb318191
 * @create 2019-05-28 9:54
 */
public class SynonymAnalyzerTest {

    @Test
    public void testSynonymAnalyzer() throws IOException {
        SynonymAnalyzer analyzer = new SynonymAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("contents", new StringReader("The quick brown fox"));
        tokenStream.reset();

        TypeAttribute typeAttribute = tokenStream.getAttribute(TypeAttribute.class);
        OffsetAttribute offsetAttribute = tokenStream.getAttribute(OffsetAttribute.class);
        CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
        PositionIncrementAttribute positionIncrementAttribute = tokenStream.getAttribute(PositionIncrementAttribute.class);

        int position = 0;
        while (tokenStream.incrementToken()) {
            int positionIncrement = positionIncrementAttribute.getPositionIncrement();
            if (positionIncrement > 0) {
                position += positionIncrement;
                System.out.println();
                System.out.print(position + " : ");
            }

            System.out.printf("[%s : %d ->  %d : %s]", charTermAttribute.toString(), offsetAttribute.startOffset(), offsetAttribute.endOffset(),
                typeAttribute.type());
        }
    }
}
