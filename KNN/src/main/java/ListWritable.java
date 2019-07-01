import com.sun.istack.NotNull;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListWritable<E extends Writable> implements Writable, Iterable<E> {
    private List<E> instance;

    public ListWritable(){
        super();
    }

    public ListWritable(List<E> instance){
        this.instance = instance;
    }

    public void setInstance(List<E> instance) {
        this.instance = instance;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    class Itr implements Iterator<E> {
        int cur;

        @Override
        public boolean hasNext() {
            return cur != instance.size();
        }

        @Override
        public E next() {
            return instance.get(cur++);
        }
    }

    public void push(E e){
        if (instance == null){
            instance = new ArrayList<E>();
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
        for (E e : instance){
            sb.append(e.toString()).append(",");
        }
        String result = (String)sb.subSequence(0, sb.length()-1);
        return result + "]";
    }

    public void write(DataOutput dataOutput) throws IOException {
        //Auto-generated method stub
    }

    public void readFields(DataInput dataInput) throws IOException {
        //Auto-generated method stub
    }
}
