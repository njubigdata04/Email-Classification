import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Instance {
    // Instance contains a sparse matrix which represents the features of a email instance
    private Integer category;
    private List<Pair<Integer, Double>> attributes = new ArrayList<>();

    public Instance(String line){
        String[] list = line.split(" ");
        category = Integer.parseInt(list[0]);
        for (int i=1; i<list.length; i++){
            String[] str = list[i].split(":");
            attributes.add(new Pair<>(Integer.parseInt(str[0]), Double.parseDouble(str[1])));
        }
        sortByFeature();
    }

    public Integer getCategory() {
        return category;
    }

    public List<Pair<Integer, Double>> getAttributes() {
        return attributes;
    }

    private void sortByFeature(){
        Collections.sort(attributes, new FeatureComparator());
    }

    private class FeatureComparator implements Comparator<Pair<Integer, Double>> {
        @Override
        public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    }

    public static double EuclideanDistance(List<Pair<Integer, Double>> list1, List<Pair<Integer, Double>> list2){
        double result = 0;
        int p = 0, q = 0;
        while(p < list1.size() || q < list2.size()){
            if (p == list1.size()){
                result += Math.pow(list2.get(q).getValue(), 2);
                q++;
            }
            else if (q == list2.size()){
                result += Math.pow(list1.get(p).getValue(), 2);
                p++;
            }
            else{
                Pair<Integer, Double> p1 = list1.get(p), q1 = list2.get(q);
                if(p1.getKey() < q1.getKey()){
                    result += Math.pow(p1.getValue(), 2);
                    p++;
                }
                else if(p1.getKey() > q1.getKey()){
                    result += Math.pow(q1.getValue(), 2);
                    q++;
                }
                else{
                    result += Math.pow(p1.getValue() - q1.getValue(), 2);
                    p++; q++;
                }
            }
        }
        return Math.sqrt(result);
    }
}
