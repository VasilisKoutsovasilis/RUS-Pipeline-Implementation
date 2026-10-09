package UniversalSketch;

import org.apache.commons.collections.BufferOverflowException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RAS implements Serializable {
    private Prefilter topKPrefilter;
    private RA_SuCS sketch;

    public RAS(){
        this.topKPrefilter = new Prefilter();
        this.sketch = new RA_SuCS();
    }

    public RAS(int rows) {
        this.topKPrefilter = new Prefilter(rows);
        this.sketch = new RA_SuCS(0.01, 3, 12345);
    }

    public RAS(int rows, int slotSize){
        this.topKPrefilter = new Prefilter(rows, slotSize);
        this.sketch = new RA_SuCS();
    }

    public RAS(int rows, double epsilon, double confidence, int seed){
        this.topKPrefilter = new Prefilter(rows);
        this.sketch = new RA_SuCS(epsilon, confidence, seed);
    }

    public RAS(int rows, int slotSize, double epsilon, double confidence, int seed){
        this.topKPrefilter = new Prefilter(rows, slotSize);
        this.sketch = new RA_SuCS(epsilon, confidence, seed);
    }

    public RAS(int rows, int slotSize, double epsilon, int depth, int seed){
        this.topKPrefilter = new Prefilter(rows, slotSize);
        this.sketch = new RA_SuCS(epsilon, depth, seed);
    }

    public RAS(int rows, int slotSize, int width, int depth, int seed){
        this.topKPrefilter = new Prefilter(rows, slotSize);
        this.sketch = new RA_SuCS(width, depth, seed);
    }

    public ArrayList<Tuple3<String, Long, Long>> getTopElements(){
        ArrayList<Tuple3<String, Long, Long>> topElements = new ArrayList<>();

        for(Slot s : topKPrefilter.getKpcfTable())
            topElements.addAll(s.getTupleMinHeap());

        return topElements;
    }

    public Map<String, Long> getTopKElements(double epsilonThreshold, int option){
        Map<String, Long> topK = new HashMap<>();

        for(Tuple3<String, Long, Long> elem: getTopElements())
            if(g(elem.f1, option) >= epsilonThreshold)
                topK.put(elem.f0, elem.f1);

        return topK;
    }

    public void updateRas(String key, int weight){
        int[] hashTuple = topKPrefilter.cuckooHash(key);

        Tuple2<Integer, Integer> u_v = topKPrefilter.lookupFilter(key, hashTuple[0], hashTuple[1]);
        Tuple3<String, Long, Long> kickedElem = Tuple3.of(null, 0L, 0L);

        if(u_v.f1 != null && u_v.f1 != -1 && topKPrefilter.getFromSlot(u_v.f0, u_v.f1).f0.equals(key)){
            long newFreq = weight + topKPrefilter.getFromSlot(u_v.f0, u_v.f1).f1;
            topKPrefilter.updateFilter(key, u_v.f0, u_v.f1, newFreq);
        }else if(u_v.f1 != null && u_v.f1 == -1){
            kickedElem = topKPrefilter.insertFilter(u_v.f0, key, weight, 0L);
        }else{
            sketch.add(key, weight);
            long newFreq = sketch.estimateCount(key);

            if(newFreq > topKPrefilter.getApproxGlobalMin())
                kickedElem = topKPrefilter.insertFilter(u_v.f0, key, newFreq, newFreq);

        }

        if(kickedElem.f0 != null)
            sketch.add(kickedElem.f0, kickedElem.f1-kickedElem.f2);

    }

    public long getPerElementFrequency(String key){
        int[] hashVals =  topKPrefilter.cuckooHash(key);

        int s1 = topKPrefilter.getKpcfTable()[hashVals[0]].search(key);

        if(s1 != -1)
            return topKPrefilter.getFromSlot(hashVals[0], s1).f1;

        int s2 = topKPrefilter.getKpcfTable()[hashVals[1]].search(key);

        if(s2 != -1)
            return topKPrefilter.getFromSlot(hashVals[1], s2).f1;

        return sketch.estimateCount(key);
    }

    public boolean elemInPrefilter(String key){
        int[] hashVals =  topKPrefilter.cuckooHash(key);

        int s1 = topKPrefilter.getKpcfTable()[hashVals[0]].search(key);

        if(s1 != -1)
            return true;

        int s2 = topKPrefilter.getKpcfTable()[hashVals[1]].search(key);

        return s2 != -1;
    }

    public double g(long f, int option){
        if(f <= 0) return 0;

        switch (option){
            case 0: return 1;
            case 1: return f;
            case 2: return (double)f*f;
            case 3: return Math.pow(f, 3);
            case 4: return f*(Math.log(f)/Math.log(2));
            default: return -1;
        }
    }

    public long getFreqFromKey(String key){
        int[] hashTuple = topKPrefilter.cuckooHash(key);
        int h1 = hashTuple[0];
        int h2 = hashTuple[1];

        Tuple2<Integer, Integer> u_v = topKPrefilter.lookupFilter(key, h1, h2);
        if(u_v.f0 != null && (u_v.f1 != null && u_v.f1 != -1))
            return topKPrefilter.getFromSlot(u_v.f0, u_v.f1).f1;
        else
            return -1;
    }

    public boolean canMerge(RAS other){
        if(other != null){
            if(!this.sketch.canMerge(other.getSketch()))
                return false;
            return this.topKPrefilter.canMerge(other.getTopKPrefilter());
        }
        return false;
    }
    public ArrayList<Tuple3<String, Long, Long>> merge(RAS ras) throws BufferOverflowException {
        this.sketch.merge(ras.getSketch());
        return this.topKPrefilter.merge(ras.getTopKPrefilter());
    }

    public RA_SuCS getSketch() {
        return sketch;
    }

    public void setSketch(RA_SuCS sketch) {
        this.sketch = sketch;
    }

    public Prefilter getTopKPrefilter() {
        return topKPrefilter;
    }

    public void setTopKPrefilter(Prefilter topKPrefilter) {
        this.topKPrefilter = topKPrefilter;
    }


}

