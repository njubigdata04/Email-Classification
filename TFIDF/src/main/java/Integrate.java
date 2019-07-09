import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class Integrate {
    public static class IntegrateMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
        }
    }

    public static class IntegrateReducer extends Reducer<Text, Text, Text, Text> {

        private double keywordIDF = 0.0d;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }

            if (!key.toString().contains(":")) {
                keywordIDF = Double.parseDouble(values.iterator().next().toString());
                return;
            }

            Text value = new Text(String.valueOf(Double.parseDouble(values.iterator().next().toString()) * keywordIDF));

            String[] nameSegments = key.toString().split(":");
            String s = nameSegments[1] + ":" + nameSegments[0];

            context.write(new Text(s), value);
        }
    }
/*
    public static class IntegratePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            if (key.toString().contains(":")) {
                String fileName = key.toString().split(":")[1];
                return Math.abs((fileName.hashCode() * 127) % numPartitions);
            } else {
                return Math.abs((key.toString().hashCode() * 127) % numPartitions);
            }
        }
    }
*/
}