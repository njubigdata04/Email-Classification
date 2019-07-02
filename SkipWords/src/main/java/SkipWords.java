import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
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

    public static String tmpFilePath = "/task3-tmp-out";

    public static class SkipWordsMapper extends Mapper<Object, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();
        private String[] sws;
        private Analyzer analyzer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path[] path = DistributedCache.getLocalCacheFiles(conf);
            FileSystem fs = FileSystem.getLocal(conf);
            FSDataInputStream in = fs.open(path[0]);
            Scanner scan = new Scanner(in);
            ArrayList<String> arrayList = new ArrayList<String>();
            while (scan.hasNext()) {
                String s = scan.next();
                arrayList.add(s);
            }
            scan.close();
            in.close();
            sws = new String[arrayList.size()];
            for (int i = 0; i < arrayList.size(); i++) {
                sws[i] = arrayList.get(i);
            }
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

            Configuration conf = context.getConfiguration();
            String uri = tmpFilePath + "/" + fileName;

            FileSystem fs1 = FileSystem.newInstance(URI.create(uri), conf);
            FSDataOutputStream out = null;
            Path path = new Path(uri);
            if (fs1.exists(path)){
                out = fs1.append(new Path(uri));
            } else {
                out = fs1.create(new Path(uri));
            }


            while (stream.incrementToken()) {
                String s = cta.toString();
                label.set(s + ":" + fileName);
                context.write(label, one);
                out.writeChars(s + " ");
            }
            stream.end();
            stream.close();
            fs1.close();
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
        String inputPath = "/task3/email";
        String stopWordsPath = "/task3/Stop_words.txt";
        String outputPath = "/task3-out";
        tmpFilePath = "/task3-tmp-out";
        if (args.length >= 4) {
            inputPath = args[0];
            stopWordsPath = args[1];
            outputPath = args[2];
            tmpFilePath = args[3];
        } else {
            System.out.println("args error");
            System.exit(-1);
            return;
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
        DistributedCache.addCacheFile(new URI(stopWordsPath), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        /*
            String uri = "/fff-out/2.txt";
            Configuration configuration = new Configuration();
            FileSystem fs1 = FileSystem.get(URI.create(uri), configuration);
            FSDataOutputStream out = null;
            try {
                out = fs1.create(new Path(uri));
                out.writeUTF("fffsssxzx");
                fs1.close();
            } finally {
            }
*/
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