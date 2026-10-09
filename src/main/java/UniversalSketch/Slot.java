package UniversalSketch;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Slot implements Serializable {
    private final ArrayList<Tuple3<String, Long, Long>> tupleMinHeap;
    private int maxSize;
    public static final int DEFAULT_MAX_SIZE = 8;

    public Slot() {
        this.maxSize = DEFAULT_MAX_SIZE;
        this.tupleMinHeap = new ArrayList<>(this.maxSize);
    }

    public Slot(int maxSize){
        if(maxSize <= 0)
            this.maxSize = DEFAULT_MAX_SIZE;
        else
            this.maxSize = maxSize;
        this.tupleMinHeap = new ArrayList<>(this.maxSize);
    }

    private void swap(int i, int j){
        Tuple3<String, Long, Long> temp = tupleMinHeap.get(i);
        tupleMinHeap.set(i, tupleMinHeap.get(j));
        tupleMinHeap.set(j, temp);
    }

    public ArrayList<Tuple3<String, Long, Long>> getTupleMinHeap() {
        return tupleMinHeap;
    }

    public boolean hasSpace(){
        return (this.tupleMinHeap.size() < this.maxSize);
    }

    private void heapify(ArrayList<Tuple3<String, Long, Long>> heap, int i, int n){
        int smallest = i;
        int left = 2*i+1;
        int right = 2*i+2;

        if (left < n && heap.get(left).f1 < heap.get(smallest).f1)
            smallest = left;

        if (right < n && heap.get(right).f1 < heap.get(smallest).f1)
            smallest = right;

        if (smallest != i) {
            swap(i, smallest);
            heapify(heap, smallest, n);
        }
    }

    public boolean insert(Tuple3<String, Long, Long> evt){
        if (this.hasSpace()){
            tupleMinHeap.add(evt);

            int index = tupleMinHeap.size()-1;
            while(index > 0 && tupleMinHeap.get((index-1)/2).f1 > tupleMinHeap.get(index).f1){
                swap(index, (index-1)/2);
                index = (index-1)/2;
            }

            return true;
        }

        return false;
    }

    public Tuple3<String, Long, Long> peek(){
        if(!tupleMinHeap.isEmpty()) {
            Tuple3<String, Long, Long> root = tupleMinHeap.get(0);
            return Tuple3.of(root.f0, root.f1, root.f2);
        }

        return null;
    }

    public int search(String key){
        for(int i = 0; i < tupleMinHeap.size(); i++)
            if (tupleMinHeap.get(i).f0.equals(key))
                return i;

        return -1;
    }

    public boolean delete(String key){
        int index = search(key);
        if(index == -1)
            return false;

        swap(index, tupleMinHeap.size()-1);
        tupleMinHeap.remove(tupleMinHeap.size()-1);

        if(index < tupleMinHeap.size()) {
            heapify(tupleMinHeap, index, tupleMinHeap.size());
            while(index > 0 && tupleMinHeap.get((index-1)/2).f1 > tupleMinHeap.get(index).f1){
                swap(index, (index-1)/2);
                index = (index-1)/2;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return tupleMinHeap.toString();
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }
}
