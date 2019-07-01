import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KNNMapper extends Mapper<LongWritable, Text, LongWritable, ListWritable<DoubleWritable>> {
    private List<Instance> trainList = new ArrayList<>();
    private static int K = 10;

    public static void setK(int k) {
        K = k;
    }

    private int IndexOfMax(List<Double> distance){
        int result = 0;
        for (int i = 1; i<distance.size(); i++){
            if(distance.get(i) > distance.get(result)){
                result  = i;
            }
        }
        return result;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Read trainList in Distributed Cache
        try{
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0){
                String line;
                String[] tokens;
                try (BufferedReader dataReader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
                    while ((line = dataReader.readLine()) != null) {
                        //TODO: read a Instance, split and add to trainList
                        trainList.add(new Instance(line));
                    }
                }
            } // end of if
        } catch (IOException e){
            System.err.println("Exception reading DistributedCache: " + e);
        }
    }

    @Override
    protected void map(LongWritable textIndex, Text textLine, Context context) throws IOException, InterruptedException {
        ArrayList<Double> distance = new ArrayList<>(K);
        ArrayList<DoubleWritable> trainLable = new ArrayList<>(K);
        for (int i=0; i<K; i++){
            distance.add(Double.MAX_VALUE);
            trainLable.add(new DoubleWritable(-1.0));
        }
        ListWritable<DoubleWritable> lables = new ListWritable<>();

        Instance testInstance = new Instance(textLine.toString());
        for (Instance instance : trainList) {
            try {
                double dis = Instance.EuclideanDistance(instance.getAttributes(), testInstance.getAttributes());
                int index = IndexOfMax(distance);
                if (dis < distance.get(index)) {
                    distance.remove(index);
                    trainLable.remove(index);
                    distance.add(dis);
                    trainLable.add(new DoubleWritable(instance.getCategory()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        lables.setInstance(trainLable);
        context.write(textIndex, lables);
    }
}
