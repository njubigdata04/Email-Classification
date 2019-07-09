import org.apache.commons.math3.util.Pair;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class InstanceTest {
    @Test
    public void testLineConstructor(){
        String line = "9 3620:0.2 963:0.26 439:0.156";
        Instance instance = new Instance(line);

        Integer category = instance.getCategory();
        List<Pair<Integer, Double>> attributes = instance.getAttributes();

        assertEquals(category.intValue(), 9);

        for (Pair<Integer, Double> p : attributes){
            System.out.println(p.getKey()+ " " + p.getValue());
        }
    }

    @Test
    public void testEuclideanDistance(){
        String line1 = "9 1:1 2:1 3:1";
        String line2 = "9 1:2 2:2 3:2";
        String line3 = "9 4:1 1:1";
        Instance instance1 = new Instance(line1), instance2 = new Instance(line2), instance3 = new Instance(line3);
        double result = Instance.EuclideanDistance(instance1.getAttributes(), instance2.getAttributes());
        double result1 = Instance.EuclideanDistance(instance1.getAttributes(), instance3.getAttributes());

        assertEquals(result, Math.sqrt(3), 1e-5);
        assertEquals(result1, Math.sqrt(3), 1e-5);
    }
}
