import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Predict {

    //URI stopPath = new URI(args[0]);
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
        predictJob.setJarByClass(Bayes.class);
        predictJob.addCacheFile(new Path(inputTrainPath).toUri());
        predictJob.addCacheFile(new Path(classPath).toUri());
        FileInputFormat.addInputPath(predictJob, new Path(inputTestPath));
        FileInputFormat.setInputDirRecursive(predictJob, true);
        //impliment by hadoop+
        predictJob.setMapperClass(Bayes.PredictMapper.class);
        //predictJob.setCombinerClass(Bayes.PredictCombiner.class);
        //predictJob.setPartitionerClass(Bayes.PredictPartition.class);
        predictJob.setReducerClass(Bayes.PredictReducer.class);
        FileOutputFormat.setOutputPath(predictJob, new Path(outputPath));
        predictJob.setOutputKeyClass(Text.class);
        predictJob.setOutputValueClass(Text.class);
        System.exit(predictJob.waitForCompletion(true) ? 0 : 1);
    }

}
