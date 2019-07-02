import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KNNReducer extends Reducer<LongWritable, ListWritable, NullWritable, IntWritable> {
    private IntWritable valueOfMostFrequent(ListWritable val){
        Map<Double, Integer> m = new HashMap<>();

        double result = 0;
        int maxtimes = 0;


        for (DoubleWritable dw : val) {
            double d_val = dw.get();
            if (!m.containsKey(d_val)) {
                m.put(d_val, 1);
            } else {
                int times = m.get(d_val) + 1;
                m.put(d_val, times);
                if (times > maxtimes) {
                    maxtimes = times;
                    result = d_val;
                }
            }
        }

        return new IntWritable((int)result);
    }

    @Override
    protected void reduce(LongWritable index, Iterable<ListWritable> kLables, Context context) throws IOException, InterruptedException {
        ListWritable list = kLables.iterator().next();
        IntWritable predictedLable = valueOfMostFrequent(list);
        context.write(NullWritable.get(), predictedLable);
    }

    @Test
    public void test(){
        ListWritable list = new ListWritable();
        list.push(new DoubleWritable(1.0));
        list.push(new DoubleWritable(1.0));
        list.push(new DoubleWritable(2.0));
        list.push(new DoubleWritable(2.0));
        list.push(new DoubleWritable(2.0));

        IntWritable result = valueOfMostFrequent(list);
        assertEquals(result.get(), 2);
    }
}
