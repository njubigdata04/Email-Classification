import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class TF {

    public static class TFMapper extends Mapper<Object, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();
        private int allWordCount = 0;

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String fileName = getInputSplitFileName(context.getInputSplit());
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                allWordCount++;
                label.set(String.join(":", tokenizer.nextToken(), fileName));
                context.write(label, one);
            }
            context.write(new Text("!:" + fileName), new Text(String.valueOf(allWordCount)));
        }

        /*
                @Override
                protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
                    context.write(new Text("!:" + fileName), new Text(String.valueOf(allWordCount)));
                }
        */
        private String getInputSplitFileName(InputSplit inputSplit) {
            String fileFullName = ((FileSplit) inputSplit).getPath().toString();
            String[] nameSegments = fileFullName.split("/");
            return nameSegments[nameSegments.length - 1];
        }
    }

    public static class TFCombiner extends Reducer<Text, Text, Text, Text> {
        private int allWordCount = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            if (values == null) {
                return;
            }

            if (key.toString().startsWith("!")) {
                allWordCount = Integer.parseInt(values.iterator().next().toString());
                return;
            }

            int sumCount = 0;
            for (Text value : values) {
                sumCount += Integer.parseInt(value.toString());
            }

            double tf = 1.0 * sumCount / allWordCount;
            context.write(key, new Text(String.valueOf(tf)));
        }
    }

    public static class TFReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }

            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static class TFPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String fileName = key.toString().split(":")[1];
            return Math.abs((fileName.hashCode() * 127) % numPartitions);
        }
    }


}
