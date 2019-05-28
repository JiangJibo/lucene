package org.apache.lucene.queryparser.bob.synonym;

import java.io.IOException;
import java.util.Stack;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * @author wb-jjb318191
 * @create 2019-05-27 17:48
 */
public final class SynonymFilter extends TokenFilter {

    private static final String TOKEN_TYPE_SYNONYM = "SYNONYM";

    private Stack<String> synonymStack;
    private SynonymEngine synonymEngine;
    private AttributeSource.State current;
    private final CharTermAttribute bytesTermAttribute;
    private final PositionIncrementAttribute positionIncrementAttribute;

    /**
     * Construct a token stream filtering the given input.
     *
     * @param input
     */
    public SynonymFilter(TokenStream input, SynonymEngine synonymEngine) {
        super(input);
        this.synonymEngine = synonymEngine;
        synonymStack = new Stack<>();

        this.bytesTermAttribute = addAttribute(CharTermAttribute.class);
        this.positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (!synonymStack.isEmpty()) {
            String syn = synonymStack.pop();
            restoreState(current);

            //            bytesTermAttribute.setBytesRef(new BytesRef(syn.getBytes()));
            //            bytesTermAttribute.resizeBuffer(0);
            bytesTermAttribute.append(syn);

            positionIncrementAttribute.setPositionIncrement(0);
            return true;
        }

        if (!input.incrementToken()) {
            return false;
        }

        if (addAliasesToStack()) {
            current = captureState();
        }

        return true;
    }

    private boolean addAliasesToStack() throws IOException {
        String[] synonyms = synonymEngine.getSynonyms(bytesTermAttribute.toString());
        if (synonyms == null) {
            return false;
        }
        for (String synonym : synonyms) {
            synonymStack.push(synonym);
        }
        return true;
    }
}

