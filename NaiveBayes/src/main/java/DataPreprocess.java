import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataPreprocess {
    public static class PreMapper extends Mapper<Object, Text, Text, IntWritable>{
        public void map(){
            ;
        }
    }
}
