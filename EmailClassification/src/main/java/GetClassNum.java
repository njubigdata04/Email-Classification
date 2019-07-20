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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GetClassNum {
    public static class ClassMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            FileSplit fileSplit = (FileSplit)context.getInputSplit();//获得文件信息
            String classname = splitclassname(fileSplit);//获得类名
            String filename = splitfilename(fileSplit);
            context.write(new Text(classname), new Text(filename));
        }
        private String splitclassname(FileSplit fileSplit){
            String path = fileSplit.getPath().toString();
            String[] parts = path.split("/");
            if(parts.length < 2)
                return "Wrong class" + parts[0];
            //return parts[parts.length - 2];
            return parts[parts.length - 1].split("-")[1];
        }
        private String splitfilename(FileSplit fileSplit){
            String path = fileSplit.getPath().toString();
            String[] parts = path.split("/");
            if(parts.length < 2)
                return "Wrong class" + parts[0];
            return parts[parts.length - 1].split("-")[0];
        }
    }


    public static class ClassReducer extends Reducer<Text, Text, Text, Text>{
        public Map<String, Integer> list = new HashMap<String, Integer>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Set<String> words = new HashSet<String>();
            for(Text v:values) {
                String w = v.toString();
                if(words.contains(w) == false) {
                    sum++;
                    words.add(w);
                }
            }
            list.put(key.toString(), sum);
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            int count = 0;
            for(Map.Entry<String, Integer> entry:list.entrySet()){
                String val = Integer.toString(count) + "#" + entry.getValue().toString();
                val = entry.getValue().toString();
                count++;
                context.write(new Text(entry.getKey()), new Text(val));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "get class list");
        job.setJarByClass(GetClassNum.class);
        job.setMapperClass(ClassMapper.class);
        //job.setCombinerClass(ClassCombiner.class);
        job.setReducerClass(ClassReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
