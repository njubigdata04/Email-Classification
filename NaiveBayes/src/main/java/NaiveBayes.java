import org.apache.commons.io.IOExceptionWithCause;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.*;

import static org.apache.hadoop.metrics2.impl.MsInfo.Context;

public class NaiveBayes {
    public static class MyStopAnalyzer extends Analyzer {
        private Set stops;

        public MyStopAnalyzer(String[] stopwords) {
            // 将字符串数组添加到停用词的set集合中
            stops = StopFilter.makeStopSet(stopwords, true);
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

    //获得停词表
    public static String[] GetStopWords(URI[] cacheFiles) throws IOException {
        //构建停用词表
        if (cacheFiles != null && cacheFiles.length > 0) {
            String line;
            String tokens;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[0].getPath()));
            List<String> list = new ArrayList<String>();
            while ((line = bufferedReader.readLine()) != null) {
                list.add(line);
            }
            return list.toArray(new String[list.size()]);
        } else
            return new String[0];
    }

    public static class TrainMapper extends Mapper<Object, Text, Text, IntWritable> {
        private String[] stopwords;
        private Analyzer analyzer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            //构建停用词表
            stopwords = NaiveBayes.GetStopWords(cacheFiles);
            analyzer = new MyStopAnalyzer(stopwords);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();//获得文件信息
            String classname = splitclassname(fileSplit);//获得类名
            TokenStream stream = analyzer.tokenStream("renyi", new StringReader(value.toString()));
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                String k = classname + "#" + cta.toString();
                context.write(new Text(k), new IntWritable(1));
            }
            stream.end();
            stream.close();
        }

        private String splitclassname(FileSplit fileSplit) {
            String path = fileSplit.getPath().toString();
            String[] parts = path.split("/");
            if (parts.length < 2)
                return "Wrong class" + parts[0];
            return parts[parts.length - 2];
        }
    }

    public static class TrainCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException, IOException {
            int sum = 0;
            for (IntWritable v : values) {
                sum = sum + v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TrainReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new Text(Integer.toString(sum)));
        }
    }


    public static class PredictMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, Integer> ClassSet = new HashMap<String, Integer>();
        private Map<String, Integer> TrainSet = new HashMap<String, Integer>();
        private String[] stopwords;
        private Analyzer analyzer;
        private Set<String> WordSum = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles.length < 3)
                return;
            //第一个是停词表，第二个是类名，第三个是训练集
            stopwords = NaiveBayes.GetStopWords(cacheFiles);
            analyzer = new MyStopAnalyzer(stopwords);
            URI classURI = cacheFiles[1];
            URI trainURI = cacheFiles[2];
            if (cacheFiles != null && cacheFiles.length > 2) {
                String line;
                String tokens;
                //第一个是类名表
                BufferedReader bufferedReader = new BufferedReader(new FileReader(classURI.getPath()));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] sentences = line.split("\t");
                    String classname = sentences[0];
                    String[] sen = sentences[1].split("#");
                    ClassSet.put(classname, Integer.parseInt(sen[1]));
                }
                //第二个是训练样本集，数据集包括（类名#单词，数量）
                //其中（类名#$，数量表示词数总和）
                bufferedReader = new BufferedReader(new FileReader(trainURI.getPath()));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] sentences = line.split("\t");
                    String[] parts = sentences[0].split("#");
                    if (parts[1] != "$")
                        WordSum.add(parts[1]);
                    TrainSet.put(sentences[0], Integer.parseInt(sentences[1]));
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();//获得文件信息
            String path = fileSplit.getPath().toString();
            String[] parts = path.split("/");
            String filename;
            if (parts.length < 2)
                filename = "Wrong class";
            else
                filename = parts[parts.length - 1];
            TokenStream stream = analyzer.tokenStream("renyi", new StringReader(value.toString()));
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                String word = cta.toString();//得到单词
                for (Map.Entry<String, Integer> entry : ClassSet.entrySet()) {
                    String currentClass = entry.getKey().toString();
                    String k = currentClass + "#" + word;
                    double sumClass = ClassSet.get(currentClass).doubleValue();
                    double sumWord = WordSum.size();
                    double wordcount = 1;
                    if (TrainSet.containsKey(k)) {
                        wordcount += TrainSet.get(k);
                    }
                    double result = wordcount / (sumClass + sumWord);
                    context.write(new Text(filename + "#" + currentClass), new Text(Double.toString(result)));
                }

            }
            stream.end();
            stream.close();
        }
    }

    public static class PredictCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            double sum = 1;
            for (Text v : values) {
                String line = v.toString();
                double number = Double.parseDouble(line);
                sum *= number;
            }
            context.write(key, new Text(Double.toString(sum)));
        }
    }

    public static class PredictPartition extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {

            String term = key.toString().split("#")[0]; // <term#docid>=>term,filename
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class PredictReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Integer> ClassSet = new HashMap<String, Integer>();
        private Map<String, Double> MapResult = new HashMap<String, Double>();
        private String CurrentFile = "";

        //private Map<String, Integer> TrainSet = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            URI classURI = cacheFiles[1];
            URI trainURI = cacheFiles[2];
            if (cacheFiles != null && cacheFiles.length > 2) {
                String line;
                String tokens;
                //第一个是类名表
                BufferedReader bufferedReader = new BufferedReader(new FileReader(classURI.getPath()));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] sentences = line.split("\t");
                    String classname = sentences[0];
                    String[] sen = sentences[1].split("#");
                    ClassSet.put(classname, Integer.parseInt(sen[1]));
                }
                //第二个是训练样本集，数据集包括（类名#单词，数量）
                //其中（类名#$，数量表示词数总和）
                /*bufferedReader = new BufferedReader(new FileReader(trainURI.getPath()));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] sentences = line.split("\t");
                    TrainSet.put(sentences[0], Integer.parseInt(sentences[1]));
                }*/
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String[] parts = key.toString().split("#");
            String filename = parts[0];
            String classname = parts[1];
            if (CurrentFile.equals("")) {//第一个进入
                ;
            } else if (CurrentFile.equals(filename)) {//和原来相同
                ;
            } else {
                double max = 0;
                String cn = "";
                for (Map.Entry<String, Double> entry : MapResult.entrySet()) {
                    double count = entry.getValue();
                    if (count > max) {
                        cn = entry.getKey();
                        max = count;
                    }
                }
                context.write(new Text(filename), new Text(cn));
                MapResult.clear();
            }
            CurrentFile = filename;
            double number = 0;
            if (MapResult.containsKey(classname))
                number = MapResult.get(classname);
            else
                number = 1;
            for (Text v:values) {
                double tmp = Double.parseDouble(v.toString());
                number*=tmp;
            }
            MapResult.put(classname, number);
        }
    }

    public static void tmpmain(String[] args) throws Exception{
        URI stopPath = new URI(args[0]);
        URI classPath = new URI(args[1]);
        String inputTrainPath = args[2];
        String inputTestPath = args[3];
        String outputPath = args[4];
        Configuration conf = new Configuration();
        Path tmpDir = new Path("Bayes-tmp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        //Path tmpDir = new Path ("/tmp/2019st04/tmpdir");
        Job job = Job.getInstance(conf, "Train Naive Bayes");
        job.setJarByClass(NaiveBayes.class);
        try {
            job.setJarByClass(NaiveBayes.class);
            job.setMapperClass(TrainMapper.class);
            job.setCombinerClass(TrainCombiner.class);
            job.setCombinerClass(TrainCombiner.class);
            job.setReducerClass(TrainReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputTrainPath));
            FileOutputFormat.setOutputPath(job, tmpDir);
            //job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if (job.waitForCompletion(true)) {
                Job predictJob = Job.getInstance(conf, "Predict Naive Bayes");
                predictJob.setJarByClass(NaiveBayes.class);
                FileInputFormat.addInputPath(predictJob, new Path(inputTestPath));
                //predictJob.setInputFormatClass(SequenceFileInputFormat.class);
                //impliment by hadoop
                predictJob.setMapperClass(PredictMapper.class);
                //predictJob.setNumReduceTasks(1);
                FileOutputFormat.setOutputPath(predictJob, new Path(outputPath));
                predictJob.setOutputKeyClass(Text.class);
                predictJob.setOutputValueClass(Text.class);
                System.exit(predictJob.waitForCompletion(true) ? 0 : 1);
            }

        } finally {
            FileSystem.get(conf).deleteOnExit(tmpDir);
        }
    }
    public static void main(String[] args) throws Exception {
        URI stopPath = new URI(args[0]);
        URI classPath = new URI(args[1]);
        String inputPath = args[2];
        String outputPath = args[3];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Train Naive Bayes");
        job.setJarByClass(NaiveBayes.class);
        job.setMapperClass(TrainMapper.class);
        job.setCombinerClass(TrainCombiner.class);
        job.setReducerClass(TrainReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.addCacheFile(stopPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
