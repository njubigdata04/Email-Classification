import java.io.*;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class SkipWords {

    public static class SkipWordsMapper extends Mapper<Object, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();
        private String[] sws = new String[]{"I", "you", "hate"};
        private Analyzer analyzer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            analyzer = new MyStopAnalyzer(sws);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String fileName = getInputSplitFileName(context.getInputSplit());
            TokenStream stream = null;
            stream = analyzer.tokenStream("renyi", new StringReader(value.toString()));
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
            //在lucene 4 以上  要加入reset 和  end方法
            stream.reset();
            while (stream.incrementToken()) {
                label.set(String.join(":", cta.toString(), fileName));
                context.write(label, one);
            }
            stream.end();
            stream.close();
        }

        private String getInputSplitFileName(InputSplit inputSplit) {
            String fileFullName = ((FileSplit) inputSplit).getPath().toString();
            String[] nameSegments = fileFullName.split("/");
            return nameSegments[nameSegments.length - 1];
        }
    }

    public static class SkipWordsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }

            context.write(key, new Text("1"));
        }
    }


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

    public static void main(String[] args) throws Exception {
        /*
        //String[] str = new String[]{"I", "you", "hate"};
        Analyzer analyzer = new MyStopAnalyzer(args[0]);
        Analyzer analyzer1 = new StopAnalyzer();
        String txt = "i love you, i hate you";
        //自定义的停用词分词器
        display(txt, analyzer);
        System.out.println("-----------------------");
        //默认的停用词分词器
        display(txt, analyzer1);
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

        String inputPath = "./input";
        String outputPath = "./output";
        if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        Configuration configuration = new Configuration();
        removeOutputFolder(configuration, outputPath);

        Job job = Job.getInstance(configuration);
        job.setJobName("SkipWords-job");
        job.setJarByClass(SkipWords.class);

        job.setMapperClass(SkipWordsMapper.class);
        job.setReducerClass(SkipWordsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    private static void removeOutputFolder(Configuration configuration, String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }
/*
    public static void display(String str, Analyzer a) {
        TokenStream stream = null;
        try {
            stream = a.tokenStream("renyi", new StringReader(str));
            PositionIncrementAttribute pia = stream.addAttribute(PositionIncrementAttribute.class);  //保存位置
            OffsetAttribute oa = stream.addAttribute(OffsetAttribute.class); //保存辞与词之间偏移量
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
            TypeAttribute ta = stream.addAttribute(TypeAttribute.class); //保存类型
            //在lucene 4 以上  要加入reset 和  end方法
            stream.reset();
            while (stream.incrementToken()) {
                System.out.println(pia.getPositionIncrement() + ":[" + cta.toString() + "]:" + oa.startOffset() + "->" + oa.endOffset() + ":" + ta.type());
            }
            stream.end();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
*/
}