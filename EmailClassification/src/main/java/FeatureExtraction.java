import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class FeatureExtraction {


    public static class FeatureExtractionMapper extends Mapper<Object, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            label.set(value.toString());
            context.write(label, one);
        }
    }

    public static class FeatureExtractionReducer extends Reducer<Text, Text, Text, Text> {
        private static int idx = 0;

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


    public static void main(String[] args) throws Exception {
        String inputPath = "/task3/pure-file-out";
        String outputPath = "/task3/feature-out";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        } else {
            System.out.println("args error");
            System.exit(-1);
            return;
        }


        Configuration configuration = new Configuration();
        removeOutputFolder(configuration, outputPath);

        Job job = Job.getInstance(configuration);
        job.setJobName("FeatureExtraction-job");
        job.setJarByClass(FeatureExtraction.class);

        job.setMapperClass(FeatureExtractionMapper.class);
        job.setReducerClass(FeatureExtractionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
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

}
