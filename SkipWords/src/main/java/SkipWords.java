import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.lucene.analysis.Analyzer;
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
import org.apache.lucene.analysis.util.CharArraySet;


public class SkipWords {

    public static class SkipWordsMapper extends Mapper<Object, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();
        private String[] sws;
        private Analyzer analyzer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            Path path[] = DistributedCache.getLocalCacheFiles(conf);
            FileSystem fs = FileSystem.getLocal(conf);
            FSDataInputStream in = fs.open(path[0]);
            Scanner scan = new Scanner(in);
            ArrayList<String> arrayList = new ArrayList<String>();
            while (scan.hasNext()) {
                String s = scan.next();
                System.out.println(Thread.currentThread().getName() + "扫描的内容:  " + s);
                arrayList.add(s);
            }
            scan.close();
            fs.close();
            sws = new String[arrayList.size()];
            for (int i = 0; i < arrayList.size(); i++) {
                sws[i] = arrayList.get(i);
            }

            //sws = new String[]{"I", "you"};
            analyzer = new MyStopAnalyzer(sws);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            System.out.println("in setup");
            String fileName = getInputSplitFileName(context.getInputSplit());
            TokenStream stream = null;
            stream = analyzer.tokenStream("renyi", new StringReader(value.toString()));
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
            //在lucene 4 以上  要加入reset 和  end方法
            stream.reset();
            while (stream.incrementToken()) {
                System.out.println(cta.toString());
                label.set(cta.toString() + ":" + fileName);
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
        private int idx = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }
            idx++;
            context.write(key, new Text(String.valueOf(idx)));
        }
    }

    public static class MyStopAnalyzer extends Analyzer {
        private Set stops;

        public MyStopAnalyzer(String[] sws) {
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
        String[] str = new String[]{"I", "you", "hate"};
        Analyzer analyzer1 = new MyStopAnalyzer(str);
        Analyzer analyzer2 = new StopAnalyzer();
        String txt = "i love you, i hate you";
        //自定义的停用词分词器
        display(txt, analyzer1);
        System.out.println("-----------------------");
        //默认的停用词分词器
        display(txt, analyzer2);
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
        */

        String inputPath = "./input";
        String cachePath = "./cache";
        String outputPath = "./output";
        if (args.length == 3) {
            inputPath = args[0];
            cachePath = args[1];
            outputPath = args[2];
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
        DistributedCache.addCacheFile(new URI(cachePath), job.getConfiguration());
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

    public static void display(String str, Analyzer a) {
        TokenStream stream = null;
        try {
            stream = a.tokenStream("renyi", new StringReader(str));
            //  PositionIncrementAttribute pia = stream.addAttribute(PositionIncrementAttribute.class);  //保存位置
            // OffsetAttribute oa = stream.addAttribute(OffsetAttribute.class); //保存辞与词之间偏移量
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
            // TypeAttribute ta = stream.addAttribute(TypeAttribute.class); //保存类型
            //在lucene 4 以上  要加入reset 和  end方法
            stream.reset();
            while (stream.incrementToken()) {
                //     System.out.println(pia.getPositionIncrement() + ":[" + cta.toString() + "]:" + oa.startOffset() + "->" + oa.endOffset() + ":" + ta.type());
                System.out.println(cta.toString());
            }
            stream.end();
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}