import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNMain {
    public static void main(String[] args){
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "KNN-MR");

            job.setJarByClass(Main.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(KNNMapper.class);
            job.setReducerClass(KNNReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(ListWritable.class);
            job.addCacheFile(new Path("/user/2019st04/cache.txt").toUri());

            //System.out.println(new Path("/cache.txt").toUri().toString());

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0:1);

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
