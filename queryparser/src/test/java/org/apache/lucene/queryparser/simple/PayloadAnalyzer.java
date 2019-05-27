package org.apache.lucene.queryparser.simple;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.util.packed.PackedInts.Reader;

/**
 * 实现解析Payload数据的Analyzer
 *
 * @author wb-jjb318191
 * @create 2019-05-27 17:03
 */
public class PayloadAnalyzer extends Analyzer {

    private PayloadEncoder encoder;

    PayloadAnalyzer(PayloadEncoder encoder) {
        this.encoder = encoder;
    }

    public TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream result = new WhitespaceTokenizer(); // 用来解析空格分隔的各个类别
        result = new DelimitedPayloadTokenFilter(result, '|', encoder); // 在上面分词的基础上，在进行Payload数据解析
        return result;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        TokenStream result = new WhitespaceTokenizer();
        result = new DelimitedPayloadTokenFilter(result, '|', encoder); // 在上面分词的基础上，在进行Payload数据解析
        return new TokenStreamComponents(new WhitespaceTokenizer(), new DelimitedPayloadTokenFilter(result, '|', encoder));
    }
}
