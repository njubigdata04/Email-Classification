import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListWritable implements Writable, Iterable<DoubleWritable> {
    private Class<DoubleWritable> valueClass;
    private Class<? extends List> listClass;

    private List<DoubleWritable> instance = new ArrayList<>();

    public ListWritable(){
        super();
        listClass = instance.getClass();
        valueClass = DoubleWritable.class;
    }

    public ListWritable(List<DoubleWritable> instance){
        this.instance = instance;
        listClass = instance.getClass();
        valueClass = DoubleWritable.class;
    }

    public List<DoubleWritable> getInstance() {
        return instance;
    }

    @Override
    public Iterator<DoubleWritable> iterator() {
        return new Itr();
    }

    class Itr implements Iterator<DoubleWritable> {
        int cur;

        @Override
        public boolean hasNext() {
            return cur != instance.size();
        }

        @Override
        public DoubleWritable next() {
            return instance.get(cur++);
        }
    }

    public void push(DoubleWritable e){
        if (instance == null){
            instance = new ArrayList<DoubleWritable>();
        }
        instance.add(e);
    }

    public int size() {
        if (instance == null){
            return 0;
        }
        return instance.size();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (DoubleWritable e : instance){
            sb.append(e.toString()).append(",");
        }
        String result = (String)sb.subSequence(0, sb.length()-1);
        return result + "]";
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(listClass.getName());
        out.writeUTF(valueClass.getName());
        out.writeInt(instance.size()); // write values
        for (DoubleWritable doubleWritable : instance) {
            doubleWritable.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        String listClass = in.readUTF();
        try {
            this.listClass = (Class<? extends List>)Class.forName(listClass);
            String valueClass = in.readUTF();
            this.valueClass = (Class<DoubleWritable>)Class.forName(valueClass);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        int size = in.readInt(); // construct values
        try {
            instance = this.listClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < size; i++) {
            Writable value = WritableFactories.newInstance(this.valueClass);
            value.readFields(in); // read a value
            instance.add((DoubleWritable) value); // store it in values
        }
    }
}
