import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.util.CharArraySet;

public class SkipWords {

    public static String[] toArrayByFileReader(String name) {
        // 使用ArrayList来存储每行读取到的字符串
        ArrayList<String> arrayList = new ArrayList<>();
        try {
            FileReader fr = new FileReader(name);
            BufferedReader bf = new BufferedReader(fr);
            String str;
            // 按行读取字符串
            while ((str = bf.readLine()) != null) {
                arrayList.add(str);
            }
            bf.close();
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int length = arrayList.size();
        String[] array = new String[length];
        for (int i = 0; i < length; i++) {
            array[i] = arrayList.get(i);
        }
        return array;
    }


    public static class MyStopAnalyzer extends Analyzer {
        private Set stops;

        public MyStopAnalyzer(String[] sws) {
            // 将字符串数组添加到停用词的set集合中
            stops = StopFilter.makeStopSet(sws, true);
            // 加入原来的停用词
            stops.addAll(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        }

        /**
         * 默认构造方法
         */
        public MyStopAnalyzer(String filename) {
            String[] sws = toArrayByFileReader(filename);
            // 将字符串数组添加到停用词的set集合中
            stops = StopFilter.makeStopSet(sws, true);
            // 加入原来的停用词
            stops.addAll(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            //正则匹配分词
            Tokenizer source = new LowerCaseTokenizer();
            CharArraySet charArraySet = CharArraySet.copy(stops);
            return new TokenStreamComponents(source, new StopFilter(source, charArraySet));
        }

    }

    public static void main(String[] args) {
        //String[] str = new String[]{"I", "you", "hate"};
        Analyzer analyzer = new MyStopAnalyzer(args[0]);
        Analyzer analyzer1 = new StopAnalyzer();
        String txt = "i love you, i hate you";
        //自定义的停用词分词器
        display(txt, analyzer);
        System.out.println("-----------------------");
        //默认的停用词分词器
        display(txt, analyzer1);
/*
        String str = "hello, i'm a boy, and i like playing basketball" ;
        String ztr = "你好，我是一个男孩，我喜欢打篮球" ;
        Analyzer a = new StandardAnalyzer() ;      //标准分词器
        Analyzer b = new SimpleAnalyzer() ;        //简单分词器
        Analyzer c = new StopAnalyzer() ;          //停用词分词器
        Analyzer d = new WhitespaceAnalyzer() ; //空格分词器
        display(str,a) ;
        System. out.println( "-----------------------------");
        display(str,b) ;
        System. out.println( "-----------------------------");
        display(str,c) ;
        System. out.println( "-----------------------------");
        display(str,d) ;
        System. out.println( "-----------------------------");
    }
*/
    }

    public static void display(String str, Analyzer a) {
        TokenStream stream = null ;
        try {
            stream = a.tokenStream( "renyi", new StringReader(str)) ;
            PositionIncrementAttribute pia = stream.addAttribute(PositionIncrementAttribute.class ) ;  //保存位置
            OffsetAttribute oa = stream.addAttribute(OffsetAttribute.class ) ; //保存辞与词之间偏移量
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class ) ;//保存响应词汇
            TypeAttribute ta = stream.addAttribute(TypeAttribute.class ) ; //保存类型
            //在lucene 4 以上  要加入reset 和  end方法
            stream.reset() ;
            while (stream.incrementToken()) {
                System. out.println(pia.getPositionIncrement() + ":[" + cta.toString() + "]:" + oa.startOffset() + "->" + oa.endOffset() + ":" + ta.type());
            }
            stream.end() ;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}