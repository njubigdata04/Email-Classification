import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ListWritableTest {
    @Test
    public void testConstructorAndIterable() {
        List<DoubleWritable> instance = new ArrayList<>();
        instance.add(new DoubleWritable(1.0));
        instance.add(new DoubleWritable(2.0));
        instance.add(new DoubleWritable(3.0));

        ListWritable list = new ListWritable(instance);

        for (DoubleWritable d : list){
            System.out.println(d.get());
        }

        assertEquals(list.size(), 3);

        list.push(new DoubleWritable(4.0));
        for (DoubleWritable d : list){
            System.out.println(d.get());
        }

        assertEquals(list.size(), 4);
    }

    @Test
    public void testToString() {
        List<DoubleWritable> instance = new ArrayList<>();
        instance.add(new DoubleWritable(1.0));
        instance.add(new DoubleWritable(2.0));
        instance.add(new DoubleWritable(3.0));

        ListWritable list = new ListWritable(instance);

        System.out.println(list.toString());
    }
}
