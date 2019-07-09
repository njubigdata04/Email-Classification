import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.htrace.commons.logging.LogFactory;
import org.apache.htrace.commons.logging.Log;


public class TF {
    public static class TFMapper extends Mapper<Object, Text, Text, Text> {
        private static final Log LOG = LogFactory.getLog(TFMapper.class);

        private final Text one = new Text("1");
        private Text label = new Text();
        private int allWordCount = 0;
        private String fileName = "";

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            fileName = getInputSplitFileName(context.getInputSplit());
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                allWordCount++;
                //label.set(fileName + ":" + tokenizer.nextToken());
                label.set(tokenizer.nextToken() + ":" + fileName);
                LOG.error("map\t" + allWordCount);
                context.write(label, one);
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            LOG.error("cleanup\t" + allWordCount);
            //context.write(new Text(fileName + ":!"), new Text(String.valueOf(allWordCount)));
            context.write(new Text("!:" + fileName), new Text(String.valueOf(allWordCount)));
        }

        private String getInputSplitFileName(InputSplit inputSplit) {
            String fileFullName = ((FileSplit) inputSplit).getPath().toString();
            String[] nameSegments = fileFullName.split("/");
            return nameSegments[nameSegments.length - 1];
        }
    }

    public static class TFCombiner extends Reducer<Text, Text, Text, Text> {
        private int allWordCount = 0;
        private static final Log LOG = LogFactory.getLog(TFCombiner.class);

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            if (values == null) {
                return;
            }
            LOG.error("key\t" + key);
            if (key.toString().contains("!")) {
                allWordCount = Integer.parseInt(values.iterator().next().toString());
                LOG.error("all word cnt\t" + allWordCount);
                return;
            }

            int sumCount = 0;
            for (Text value : values) {
                sumCount += Integer.parseInt(value.toString());
            }

            double tf = 1.0 * sumCount / allWordCount;
            context.write(key, new Text(String.valueOf(tf)));
            //context.write(key, new Text("sumCount\t" + String.valueOf(sumCount)));
            //context.write(key, new Text("allCount\t" + String.valueOf(allWordCount)));
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
            //String fileName = key.toString().split(":")[0];
            String fileName = key.toString().split(":")[1];
            return Math.abs((fileName.hashCode() * 127) % numPartitions);
        }
    }


}