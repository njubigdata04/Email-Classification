import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class Bayes {
    public static class TrainMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();//获得文件信息
            String classname = splitclassname(fileSplit);//获得类名
            //String k = classname + "#" + value.toString();
            //context.write(new Text(k), new IntWritable(1));
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String k = classname + "#" + itr.nextToken();
                context.write(new Text(k), new IntWritable(1));
            }
        }

        private String splitclassname(FileSplit fileSplit) {
            String path = fileSplit.getPath().toString();
            String[] parts = path.split("/");
            String fileclass = parts[parts.length - 1];
            String classn = fileclass.split("-")[1];
            return classn;
            /*if (parts.length < 2)
                return "Wrong class" + parts[0];
            return parts[parts.length - 2];*/
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
        private Map<String, Integer> ClassSet = new HashMap<String, Integer>();//《类名， 类文件数量》
        private Map<String, Integer> TrainSet = new HashMap<String, Integer>();//《类名#单词，数量》《类名，词数量》
        private Set<String> WordSum = new HashSet<String>();//《词库总数》
        private static final Log LOG = LogFactory.getLog(PredictMapper.class);
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles.length < 2)
                return;
            //第一个是类名，第二个是训练集
            Path classURI = cacheFiles[1];
            Path trainURI = cacheFiles[0];

            LOG.error("/-----------------/");
            LOG.error(cacheFiles.length);
            LOG.error(trainURI.toString());
            LOG.error(classURI.toString());
            LOG.error("/-----------------/");
            if (cacheFiles != null && cacheFiles.length >= 2) {
                /*FileSystem fs = FileSystem.getLocal(context.getConfiguration());
                FSDataInputStream in = fs.open(new Path(classURI));
                Scanner scan = new Scanner(in);
                while (scan.hasNextLine()) {
                    String s = scan.next();
                    String[] sp = s.split("\t");
                    String sclass = sp[0];
                    String snum = sp[1];
                    ClassSet.put(sclass, Integer.parseInt(snum));//保存类名与文件数
                }
                scan.close();
                in.close();*/
                String line = "";
                BufferedReader bufferedReader = new BufferedReader(new FileReader(classURI.toUri().getPath()));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] sp = line.split("\t");
                    String sclass = sp[0];
                    //String snum = sp[1].split("#")[1];
                    //sclass  = sp[1].split("#")[0];
                    String snum = sp[1];
                    ClassSet.put(sclass, Integer.parseInt(snum));//保存类名与文件数
                }
                bufferedReader.close();
                //第二个是训练样本集，数据集包括（类名#单词，数量）
                //其中（类名#$，数量表示词数总和）
                bufferedReader = new BufferedReader(new FileReader(trainURI.toUri().getPath()));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] sp = line.split("\t");
                    String sclassword = sp[0];
                    String[] test = sclassword.split("#");
                    if(test.length < 2)
                    {
                        LOG.error(trainURI.toString());
                        LOG.error("/*************/");
                        LOG.error(classURI.toString());
                        LOG.error(line);
                        System.out.println(sclassword);
                        //System.exit(-1);
                    }
                    String sclass = sclassword.split("#")[0];//得到类名
                    String sword = sclassword.split("#")[1];
                    WordSum.add(sword);//构建词库
                    String snum = sp[1];
                    TrainSet.put(sclassword, Integer.parseInt(snum));//保存类名与单词计数
                    //构建类名的词数之和
                    if(TrainSet.containsKey(sclass))
                    {
                        int tmp = TrainSet.get(sclass);
                        tmp += Integer.parseInt(snum);
                        TrainSet.put(sclass, tmp);
                    }
                    else
                        TrainSet.put(sclass, Integer.parseInt(snum));
                }
                bufferedReader.close();
                /*in = fs.open(new Path(trainURI));
                scan = new Scanner(in);
                while (scan.hasNextLine()) {
                    String s = scan.next();
                    String[] sp = s.split("\t");
                    String sclassword = sp[0];
                    String sclass = sclassword.split("#")[0];//得到类名
                    String sword = sclassword.split("#")[1];
                    WordSum.add(sword);//构建词库
                    String snum = sp[1];
                    TrainSet.put(sclassword, Integer.parseInt(snum));//保存类名与单词计数
                    //构建类名的词数之和
                    if(TrainSet.containsKey(sclass))
                    {
                        int tmp = TrainSet.get(sclass);
                        tmp += Integer.parseInt(snum);
                        TrainSet.put(sclass, tmp);
                    }
                    else
                        TrainSet.put(sclass, Integer.parseInt(snum));
                }
                scan.close();
                in.close();*/
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();//获得文件信息
            String filename = splitfilename(fileSplit);//获得文件名
            String word = value.toString();
            for (Map.Entry<String, Integer> entry : ClassSet.entrySet()) {
                String currentClass = entry.getKey().toString();
                String k = currentClass + "#" + word;
                double sumClass = ClassSet.get(currentClass).doubleValue();
                sumClass = TrainSet.get(currentClass).doubleValue();
                double sumWord = WordSum.size();
                double wordcount = 1;
                if (TrainSet.containsKey(k)) {
                    wordcount += TrainSet.get(k);
                }
                double result = wordcount / (sumClass + sumWord);
                //context.write(new Text(filename + "#" + currentClass), new Text(Double.toString(result)));
                context.write(new Text(filename), new Text(currentClass + "#" + Double.toString(result)));
            }

        }
        private String splitfilename(FileSplit fileSplit){
            String path = fileSplit.getPath().toString();
            String[] parts = path.split("/");
            if (parts.length < 2)
                return "Wrong class" + parts[0];
            return parts[parts.length - 1];
        }
    }

    public static class PredictCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            double sum = 1;
            for (Text v : values) {
                String line = v.toString();
                //double number = Double.parseDouble(line);
                double number = Double.parseDouble(line.split("#")[1]);
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
        private double SumFile = 0;
        private int Correct = 0;
        //private Map<String, Integer> TrainSet = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles.length < 2)
                return;
            //第一个是类名，第二个是训练集
            Path classURI = cacheFiles[1];
            //URI trainURI = cacheFiles[1];
            if (cacheFiles != null && cacheFiles.length >= 2) {
                /*FileSystem fs = FileSystem.getLocal(context.getConfiguration());
                FSDataInputStream in = fs.open(new Path(classURI));
                Scanner scan = new Scanner(in);
                while (scan.hasNextLine()) {
                    String s = scan.next();
                    String[] sp = s.split("\t");
                    String sclass = sp[0];
                    String snum = sp[1];
                    ClassSet.put(sclass, Integer.parseInt(snum));//保存类名与文件数
                    SumFile += Double.parseDouble(snum);
                }
                scan.close();
                in.close();*/
                String line = "";
                BufferedReader bufferedReader = new BufferedReader(new FileReader(classURI.toString()));
                while ((line = bufferedReader.readLine()) != null) {
                    String[] sp = line.split("\t");
                    String sclass = sp[0];
                    //String snum = sp[1].split("#")[1];
                    String snum = sp[1];
                    ClassSet.put(sclass, Integer.parseInt(snum));//保存类名与文件数
                    SumFile += Double.parseDouble(snum);
                }
            }

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*int sum = 0;
            Text V = new Text("sa");
            for(Map.Entry<String, Integer> cs:ClassSet.entrySet()){
                context.write(new Text(cs.getKey()), new Text(cs.getValue().toString()));
            }
            //context.write(key, V);
            return;*/
            MapResult.clear();
            String filename = key.toString();
            String classname = "";
            //保存所有的类
            for(Text value: values){
                String[] parts = value.toString().split("#");
                classname = parts[0];
                double v = Double.parseDouble(parts[1]);
                double number = 0;
                if (MapResult.containsKey(classname))
                    number = MapResult.get(classname);
                else if(ClassSet.containsKey(classname))
                    number = ClassSet.get(classname) / SumFile;
                else
                {
                    System.out.println("/****************\n" + classname + "\n" + SumFile);
                    number = ClassSet.get(classname) / SumFile;
                }
                number *= v;
                MapResult.put(classname, number);
            }
            //找出最大的类
            double max = 0;
            String cn = "No find";
            for(Map.Entry<String, Double> entry:MapResult.entrySet()){
                double count = entry.getValue();
                if (count > max) {
                    cn = entry.getKey();
                    max = count;
                }
            }
            context.write(new Text(filename), new Text(cn));
            MapResult.clear();
            String[] sp = filename.split("-");
            if(sp[1].equals(cn))
                Correct++;
            /*String[] parts = key.toString().split("#");
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
                context.write(new Text(CurrentFile), new Text(cn));
                MapResult.clear();
            }
            CurrentFile = filename;
            double number = 0;
            if (MapResult.containsKey(classname))
                number = MapResult.get(classname);
            else if(ClassSet.containsKey(classname))
                number = ClassSet.get(classname) / SumFile;
            else
            {
                System.out.println("/****************\n" + classname + "\n" + SumFile);
                number = ClassSet.get(classname) / SumFile;
            }
            for (Text v:values) {
                double tmp = Double.parseDouble(v.toString());
                number*=tmp;
            }
            MapResult.put(classname, number);*/
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            /*double max = 0;
            String cn = "";
            for (Map.Entry<String, Double> entry : MapResult.entrySet()) {
                double count = entry.getValue();
                if (count > max) {
                    cn = entry.getKey();
                    max = count;
                }
            }
            context.write(new Text(CurrentFile), new Text(cn));
            MapResult.clear();*/
            context.write(new Text("Correct"), new Text(Integer.toString(Correct)));
        }
    }

    public static void main(String[] args) throws Exception{
        //URI stopPath = new URI(args[0]);
        String classPath = args[0];
        String inputTrainPath = args[1];
        String inputTestPath = args[2];
        String outputPath = args[3];
        Configuration conf = new Configuration();
        String tmpDirStr = "/Bayes-tmp";
        Path tmpDir = new Path(tmpDirStr);
        //Path tmpDir = new Path ("/tmp/2019st04/tmpdir");
        Job job = Job.getInstance(conf, "Train Naive Bayes");

        job.setJarByClass(NaiveBayes.class);
        try {
            job.addCacheFile(new Path(classPath).toUri());
            job.setJarByClass(Bayes.class);
            job.setMapperClass(TrainMapper.class);
            job.setCombinerClass(TrainCombiner.class);
            job.setCombinerClass(TrainCombiner.class);
            job.setReducerClass(TrainReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputTrainPath));
            FileInputFormat.setInputDirRecursive(job, true);
            FileOutputFormat.setOutputPath(job, tmpDir);
            //job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if (job.waitForCompletion(true)) {
                conf = new Configuration();
                Job predictJob = Job.getInstance(conf, "Predict Naive Bayes");
                predictJob.setJarByClass(Bayes.class);

                predictJob.addCacheFile(new Path(tmpDirStr + "/part-r-00000").toUri());
                predictJob.addCacheFile(new Path(classPath).toUri());

                FileInputFormat.addInputPath(predictJob, new Path(inputTestPath));
                FileInputFormat.setInputDirRecursive(predictJob, true);
                //impliment by hadoop+
                predictJob.setMapperClass(PredictMapper.class);
                predictJob.setCombinerClass(PredictCombiner.class);
                predictJob.setPartitionerClass(PredictPartition.class);
                predictJob.setReducerClass(PredictReducer.class);
                FileOutputFormat.setOutputPath(predictJob, new Path(outputPath));
                predictJob.setOutputKeyClass(Text.class);
                predictJob.setOutputValueClass(Text.class);
                System.exit(predictJob.waitForCompletion(true) ? 0 : 1);
            }

        } finally {
            FileSystem.get(conf).deleteOnExit(tmpDir);
        }
    }
}
