import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Client {
    private static String inputPath = "./input";
    private static String outputPath = "./output";

    public static void main(String[] args) {
        Client client = new Client();

        if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        }

        try {
            client.execute();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    private static void removeOutputFolder(Configuration configuration, String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    private void execute() throws Exception {
        String tmpTFPath = outputPath + "_tf";
        String tmpIDFPath = outputPath + "_idf";

        runTFJob(inputPath, tmpTFPath);
        runIDFJob(tmpTFPath, tmpIDFPath);
        runIntegrateJob(tmpTFPath, tmpIDFPath, outputPath);
    }

    private int runTFJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        removeOutputFolder(configuration, outputPath);

        Job job = Job.getInstance(configuration);
        job.setJobName("TF-job");
        job.setJarByClass(TF.class);

        job.setMapperClass(TF.TFMapper.class);
        job.setCombinerClass(TF.TFCombiner.class);
        job.setPartitionerClass(TF.TFPartitioner.class);
        job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
        job.setReducerClass(TF.TFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int runIDFJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        removeOutputFolder(configuration, outputPath);

        Job job = Job.getInstance(configuration);
        job.setJobName("IDF-job");
        job.setJarByClass(IDF.class);

        job.setMapperClass(IDF.IDFMapper.class);
        job.setReducerClass(IDF.IDFReducer.class);
//        job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
        job.setProfileParams(String.valueOf(getNumReduceTasks(configuration, inputPath)));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int runIntegrateJob(String inputTFPath, String inputIDFPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        removeOutputFolder(configuration, outputPath);

        Job job = Job.getInstance(configuration);
        job.setJobName("Integrate-job");
        job.setJarByClass(Integrate.class);

        job.setMapperClass(Integrate.IntegrateMapper.class);
        job.setReducerClass(Integrate.IntegrateReducer.class);
      //  job.setPartitionerClass(Integrate.IntegratePartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputTFPath));
        FileInputFormat.addInputPath(job, new Path(inputIDFPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int getNumReduceTasks(Configuration configuration, String inputPath) throws Exception {
        FileSystem hdfs = FileSystem.get(configuration);
        FileStatus status[] = hdfs.listStatus(new Path(inputPath));
        return status.length;
    }
}