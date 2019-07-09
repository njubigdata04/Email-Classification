import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Train {
    public static void main(String[] args) throws Exception {
        //URI stopPath = new URI(args[0]);
        String classPath = args[0];
        String inputTrainPath = args[1];
        //String inputTestPath = args[2];
        String outputPath = args[2];
        Configuration conf = new Configuration();
        //String tmpDirStr = "/Bayes-tmp";
        //Path tmpDir = new Path(tmpDirStr);
        //Path tmpDir = new Path ("/tmp/2019st04/tmpdir");
        Job job = Job.getInstance(conf, "Train Naive Bayes");

        job.setJarByClass(Train.class);

        job.addCacheFile(new Path(classPath).toUri());
        job.setJarByClass(Bayes.class);
        job.setMapperClass(Bayes.TrainMapper.class);
        job.setCombinerClass(Bayes.TrainCombiner.class);
        job.setCombinerClass(Bayes.TrainCombiner.class);
        job.setReducerClass(Bayes.TrainReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputTrainPath));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
