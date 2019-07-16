import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;

public class Test {
    public static class TestMapper extends Mapper<Object, Text, Text, Text> {
        private String file1;
        private String file2;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles.length < 2)
                return;
            //第一个是类名，第二个是训练集
            Path classURI = cacheFiles[0];
            Path trainURI = cacheFiles[1];
            String line = "";
            BufferedReader bufferedReader = new BufferedReader(new FileReader(classURI.toUri().getPath()));
            while ((line = bufferedReader.readLine()) != null) {
                file1 = file1 + line;
            }
            bufferedReader.close();
            bufferedReader = new BufferedReader(new FileReader(trainURI.toUri().getPath()));
            while ((line = bufferedReader.readLine()) != null) {
                file2 = file2 + line;
            }
            bufferedReader.close();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();//获得文件信息
            //String classname = splitclassname(fileSplit);//获得类名
            context.write(new Text("file1"), new Text(file1));
            context.write(new Text("file2"), new Text(file2));

        }
    }


    public static class TestReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String sum = "";
            for (Text v : values) {
                sum = v.toString();
            }
            context.write(key, new Text(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        String classPath = args[0];
        String inputTrainPath = args[1];
        String inputTestPath = args[2];
        String outputPath = args[3];
        Configuration conf = new Configuration();
        //String tmpDirStr = "/Bayes-tmp";
        //Path tmpDir = new Path(tmpDirStr);
        //Path tmpDir = new Path ("/tmp/2019st04/tmpdir");

        Job predictJob = Job.getInstance(conf, "Predict Naive Bayes");
        predictJob.setJarByClass(Test.class);
        predictJob.addCacheFile(new Path(inputTrainPath).toUri());
        predictJob.addCacheFile(new Path(classPath).toUri());
        FileInputFormat.addInputPath(predictJob, new Path(inputTestPath));
        FileInputFormat.setInputDirRecursive(predictJob, true);
        //impliment by hadoop+
        predictJob.setMapperClass(TestMapper.class);
        predictJob.setReducerClass(TestReducer.class);
        FileOutputFormat.setOutputPath(predictJob, new Path(outputPath));
        predictJob.setOutputKeyClass(Text.class);
        predictJob.setOutputValueClass(Text.class);
        System.exit(predictJob.waitForCompletion(true) ? 0 : 1);
    }

}
