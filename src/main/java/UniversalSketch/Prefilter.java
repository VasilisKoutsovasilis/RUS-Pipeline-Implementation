package UniversalSketch;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Prefilter implements Serializable {
    private int[] tinyHistogram;
    private Slot[] kpcfTable;
    public static final int totalMaxKicks = 8;
    private int rows;
    private int m;
    public static final int DEFAULT_ROW_SIZE = 50;
    public static final int TINY_HISTOGRAM_SIZE = 28;

    public Prefilter() {
        this.tinyHistogram = new int[28];
        this.rows = DEFAULT_ROW_SIZE;
        this.m = -1;
        this.kpcfTable = new Slot[DEFAULT_ROW_SIZE];
        for(int i = 0; i < DEFAULT_ROW_SIZE; i++)
            this.kpcfTable[i] = new Slot();

    }

    public Prefilter(int rows){
        this.tinyHistogram = new int[28];
        this.rows = rows;
        this.m = -1;
        this.kpcfTable = new Slot[rows];
        for(int i = 0; i < rows; i++)
            this.kpcfTable[i] = new Slot();

    }

    public Prefilter(int rows, int slotSize){
        this.tinyHistogram = new int[TINY_HISTOGRAM_SIZE];
        this.rows = rows;
        this.m = -1;
        this.kpcfTable = new Slot[rows];
        for(int i = 0; i < rows; i++) {
            this.kpcfTable[i] = new Slot(slotSize);
        }
    }

    public void histogramInsert(long f){
        if(f <= 0)
            return;

        int index = Math.min(63-Long.numberOfLeadingZeros(f), tinyHistogram.length-1);
        this.tinyHistogram[index]++;

        if(m==-1 || index < m)
            m = index;
    }

    public void histogramDelete(long f){
        if(f <= 0)
            return;

        int index = Math.min(63-Long.numberOfLeadingZeros(f), tinyHistogram.length-1);
        this.tinyHistogram[index]--;

        if(tinyHistogram[m] == 0)
            updateM();
    }

    private void histogramUpdate(long olfFreq, long newFreq){
        histogramDelete(olfFreq);
        histogramInsert(newFreq);
    }

    public int getApproxGlobalMin() {
        return (m == -1) ? 0 : 1 << m;
    }

    private void updateM(){
        for(int i = 0; i < TINY_HISTOGRAM_SIZE; i++){
            if(tinyHistogram[i] != 0){
                this.m = i;
                return;
            }
        }
        this.m = -1;
    }

    public void updateFilter(String key, int u, int v, long newFreq){
        long oldFreq = getFromSlot(u, v).f1;
        long cumFreq = getFromSlot(u, v).f2;

        histogramUpdate(oldFreq, newFreq);
        kpcfTable[u].delete(key);
        kpcfTable[u].insert(Tuple3.of(key, newFreq, cumFreq));
    }

    public Tuple2<Integer, Integer> lookupFilter(String key, int h1, int h2){
        // search for key in the min heaps
        int s1 = kpcfTable[h1].search(key);
        int s2 = kpcfTable[h2].search(key);

        /* if the element exists return its index in the kpcf table
           and its index in the min heap */
        if(s1 != -1)
            return Tuple2.of(h1, s1);
        if(s2 != -1)
            return Tuple2.of(h2, s2);

        // if the element doesn't exist check if either min heap has space for it
        if(kpcfTable[h1].hasSpace())
            return Tuple2.of(h1, -1);

        if(kpcfTable[h2].hasSpace())
            return Tuple2.of(h2, -1);

        /* if the element doesn't exist and there is no space return the index
           of the kpcf having the lesser frequency of the two min-heap roots*/
        if(kpcfTable[h1].peek().f1 < kpcfTable[h2].peek().f1)
            return Tuple2.of(h1, null);
        else
            return Tuple2.of(h2, null);

    }

    public int[] cuckooHash(String key) {
        byte[] bytes = key.getBytes();
        int unbound_h1 = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0);
        int salt_hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 1357);
        int unbound_h2 = unbound_h1 ^ salt_hash;
        int h1 = Math.floorMod(unbound_h1, this.rows);
        int h2 = Math.floorMod(unbound_h2, this.rows);
        if(h1 == h2)
            h2 = (h1+1) % this.rows;

        return new int[]{h1, h2};
    }

    public Tuple3<String, Long, Long> insertFilter(int u, String key, long newFreq, long oldFreq){
        // index to move, key, newFreq, oldFreq, curr index
        ArrayList<Tuple5<Integer, String, Long, Long, Integer>> preKickingQueue = new ArrayList<>();

        Tuple3<String, Long, Long> replacedElem = Tuple3.of(null, 0L, 0L);
        preKickingQueue.add(Tuple5.of(u, key, newFreq, oldFreq, -1));
        int maxKicks = Math.min(rows-1, totalMaxKicks);
        int kickCount = 0;

        int idx = u;
        while(!kpcfTable[idx].hasSpace() && kickCount < maxKicks){
            kickCount++;
            Tuple3<String, Long, Long> elemOnKickPath = kpcfTable[idx].peek();
            int[] h1_h2 = cuckooHash(elemOnKickPath.f0);
            int idxToMove = (h1_h2[0] == idx) ? h1_h2[1] : h1_h2[0];
            preKickingQueue.add(Tuple5.of(idxToMove, elemOnKickPath.f0, elemOnKickPath.f1, elemOnKickPath.f2, idx));
            idx = idxToMove;
        }

        if(kickCount >= maxKicks){
            long min = Long.MAX_VALUE;
            int minElemIndex = -1;
            for(int i = 0; i < preKickingQueue.size(); i++){
                if(preKickingQueue.get(i).f2 < min){
                    min = preKickingQueue.get(i).f2;
                    minElemIndex = i;
                }
            }
            if(preKickingQueue.get(minElemIndex).f1.equals(key))
                return Tuple3.of(key, newFreq, oldFreq);

            histogramDelete(preKickingQueue.get(minElemIndex).f2);
            kpcfTable[preKickingQueue.get(minElemIndex).f4].delete(preKickingQueue.get(minElemIndex).f1);
            replacedElem = Tuple3.of(preKickingQueue.get(minElemIndex).f1,
                                     preKickingQueue.get(minElemIndex).f2,
                                     preKickingQueue.get(minElemIndex).f3);
            preKickingQueue.remove(minElemIndex);
        }


        for(Tuple5<Integer, String, Long, Long, Integer> elemOnKickPath : preKickingQueue){
            // if the destination bucket has space simply insert it
            if(kpcfTable[elemOnKickPath.f0].hasSpace()){
                kpcfTable[elemOnKickPath.f0].insert(Tuple3.of(elemOnKickPath.f1, elemOnKickPath.f2, elemOnKickPath.f3));
                histogramInsert(elemOnKickPath.f2);
                break;
            }else{
                Tuple3<String, Long, Long> rootElem = kpcfTable[elemOnKickPath.f0].peek();
                histogramDelete(rootElem.f1);
                kpcfTable[elemOnKickPath.f0].delete(rootElem.f0);
                kpcfTable[elemOnKickPath.f0].insert(Tuple3.of(elemOnKickPath.f1, elemOnKickPath.f2, elemOnKickPath.f3));
                histogramInsert(elemOnKickPath.f2);
            }
        }


        return replacedElem;

    }


    public Tuple3<String, Long, Long> getFromSlot(int kpcfIndex, int minHeapIndex){
        if(kpcfTable[kpcfIndex].getTupleMinHeap().isEmpty() || kpcfTable[kpcfIndex].getTupleMinHeap().size() <= minHeapIndex)
            return Tuple3.of("", 0L , 0L);
        else
            return kpcfTable[kpcfIndex].getTupleMinHeap().get(minHeapIndex);
    }

    public int[] getTinyHistogram() {
        return tinyHistogram;
    }

    public void setTinyHistogram(int[] tinyHistogram) {
        this.tinyHistogram = tinyHistogram;
    }

    public Slot[] getKpcfTable() {
        return kpcfTable;
    }

    public void setKpcfTable(Slot[] kpcfTable) {
        this.kpcfTable = kpcfTable;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public int getM() {
        return m;
    }

    public void setM(int m) {
        this.m = m;
    }

    public int getPrefilterSize(){
        return rows*kpcfTable[0].getMaxSize();
    }

    public boolean canMerge(Prefilter other){
        if(other != null){
            if(other.getKpcfTable() == null)
                return false;
            if(other.getTinyHistogram() == null)
                return false;
            if(other.getRows() != this.rows)
                return false;
            return other.getKpcfTable()[0].getMaxSize() == this.kpcfTable[0].getMaxSize();
        }
        return false;
    }

    public ArrayList<Tuple3<String, Long, Long>> merge(Prefilter pf){
        int heapSize = this.kpcfTable[0].getMaxSize();

        ArrayList<Tuple3<String, Long, Long>> kickedElems = new ArrayList<>();
        ArrayList<Tuple4<Integer, String, Long, Long>> potentialElems = new ArrayList<>();

        for(int i = 0; i < this.rows; i++){
            this.kpcfTable[i].setMaxSize(2*heapSize);
            for(Tuple3<String, Long, Long> elem : pf.getKpcfTable()[i].getTupleMinHeap())
                this.kpcfTable[i].insert(elem);
        }

        for(int i = 0; i < TINY_HISTOGRAM_SIZE; i++)
            this.tinyHistogram[i] += pf.getTinyHistogram()[i];

        for(int i = this.rows-1; i >= 0; i--){
            while(this.kpcfTable[i].getTupleMinHeap().size() > heapSize){
                Tuple3<String, Long, Long> elem = this.kpcfTable[i].peek();
                int[] h1_h2 = cuckooHash(elem.f0);
                int idxToMove = (h1_h2[0] == i) ? h1_h2[1] : h1_h2[0];
                potentialElems.add(Tuple4.of(idxToMove, elem.f0, elem.f1, elem.f2));
                this.kpcfTable[i].delete(elem.f0);
                if(elem.f1 > 0)
                    this.tinyHistogram[Math.min(63-Long.numberOfLeadingZeros(elem.f1), tinyHistogram.length-1)]--;
            }
            this.kpcfTable[i].setMaxSize(heapSize);
        }

        this.updateM();


        for(Tuple4<Integer, String, Long, Long> pElem : potentialElems){
            if(pElem.f2 <= getApproxGlobalMin() && !this.kpcfTable[pElem.f0].hasSpace())
                kickedElems.add(Tuple3.of(pElem.f1, pElem.f2, pElem.f3));
            else {
                Tuple3<String, Long, Long> kickedElem = insertFilter(pElem.f0, pElem.f1, pElem.f2, pElem.f3);
                if (kickedElem.f0 != null)
                    kickedElems.add(kickedElem);
            }
        }

        return kickedElems;
    }


}
