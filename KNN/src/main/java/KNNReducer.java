import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KNNReducer extends Reducer<LongWritable, ListWritable<DoubleWritable>, NullWritable, DoubleWritable> {
    private DoubleWritable valueOfMostFrequent(ListWritable<DoubleWritable> val){
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

        return new DoubleWritable(result);
    }

    @Override
    protected void reduce(LongWritable index, Iterable<ListWritable<DoubleWritable>> kLables, Context context) throws IOException, InterruptedException {
        DoubleWritable predictedLable = new DoubleWritable();
        for (ListWritable<DoubleWritable> val : kLables){
            try{
                //There should be only one list for one key
                predictedLable = valueOfMostFrequent(val);
                break;
            } catch(Exception e){
                e.printStackTrace();
            }
        }
        context.write(NullWritable.get(), predictedLable);
    }
}
