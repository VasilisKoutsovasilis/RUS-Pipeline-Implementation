package UniversalSketch;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections.BufferOverflowException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RUS  implements Serializable {
    private int totalLayers;
    private RAS[] layers;
    private MomentEstimator momentEstimator;

    public RUS(){}

    public RUS(int totalLayers, boolean online){
        this.totalLayers = totalLayers;
        this.layers = new RAS[totalLayers];
        for(int i = 0; i < totalLayers; i++)
            this.layers[i] = new RAS();
        this.momentEstimator = new MomentEstimator(online);
    }

    public RUS(long distinctElems, int rows, boolean online){
        int k = rows * Slot.DEFAULT_MAX_SIZE;
        this.totalLayers = (int) (Math.ceil(Math.log(distinctElems/(double)k)/Math.log(2))+3);
        this.layers = new RAS[totalLayers];
        for(int i = 0; i < totalLayers; i++)
            this.layers[i] = new RAS(k);
        this.momentEstimator = new MomentEstimator(online);
    }

    public RUS(long distElemsOrLayers, int rows, int slotSize, boolean calcLayers, boolean online){
        if(calcLayers){
            int k = rows * slotSize;
            this.totalLayers = (int) (Math.ceil(Math.log(distElemsOrLayers/(double)k)/Math.log(2))+3);
        }else
            this.totalLayers = (int)distElemsOrLayers;

        this.layers = new RAS[totalLayers];
        for(int i = 0; i < totalLayers; i++)
            this.layers[i] = new RAS(rows, slotSize, .01, .99, (int)mix(12345+i));
        this.momentEstimator = new MomentEstimator(online);
    }

    public RUS(long distElemsOrLayers, int rows, int slotSize, double epsilon, double confidence, boolean calcLayers, boolean online){
        if(calcLayers){
            int k = rows * slotSize;
            this.totalLayers = (int) (Math.ceil(Math.log(distElemsOrLayers/(double)k)/Math.log(2))+3);
        }else
            this.totalLayers = (int)distElemsOrLayers;

        this.layers = new RAS[totalLayers];
        for(int i = 0; i < totalLayers; i++)
            this.layers[i] = new RAS(rows, slotSize, epsilon, confidence, (int)mix(12345+i));
        this.momentEstimator = new MomentEstimator(online);
    }

    public RUS(long distElemsOrLayers, int rows, int slotSize, double epsilon, int depth, boolean calcLayers, boolean online, int seed){
        if(calcLayers){
            int k = rows * slotSize;
            this.totalLayers = (int) (Math.ceil(Math.log(distElemsOrLayers/(double)k)/Math.log(2))+3);
        }else
            this.totalLayers = (int)distElemsOrLayers;
        System.out.println("Total Layers: "+totalLayers);
        this.layers = new RAS[totalLayers];
        for(int i = 0; i < totalLayers; i++)
            this.layers[i] = new RAS(rows, slotSize, epsilon, depth, (int)mix(seed+i));
        this.momentEstimator = new MomentEstimator(online);
    }

    public RUS(long distElemsOrLayers, int rows, int slotSize, int width, int depth, boolean calcLayers, boolean online, int seed){
        if(calcLayers){
            int k = rows * slotSize;
            this.totalLayers = (int) (Math.ceil(Math.log(distElemsOrLayers/(double)k)/Math.log(2))+3);
            System.out.println("Total RUS layers:"+totalLayers);
        }else
            this.totalLayers = (int)distElemsOrLayers;

        this.layers = new RAS[totalLayers];
        for(int i = 0; i < totalLayers; i++)
            this.layers[i] = new RAS(rows, slotSize, width, depth, (int)mix(seed+i));
        this.momentEstimator = new MomentEstimator(online);
    }

    public int hashFunc(String key){
        return MurmurHash3.hash32x86(key.getBytes());
    }

    private int getTopMostLayer(int hashNum){
        return Math.min(Integer.numberOfLeadingZeros(~hashNum), totalLayers-1);
    }

    public void add(String key, long w) throws BufferOverflowException{
        final int topMostLayer = getTopMostLayer(hashFunc(key));
        int oldBottomLayer = topMostLayer+1;
        int newBottomLayer = oldBottomLayer;
        long newFreq = 0;

        for(int j = topMostLayer; j >= 0; j--){
            Prefilter layerPrefilter = layers[j].getTopKPrefilter();
            int[] hashTuple = layerPrefilter.cuckooHash(key);
            int h1 = hashTuple[0];
            int h2 = hashTuple[1];

            Tuple2<Integer, Integer> u_v = layerPrefilter.lookupFilter(key, h1, h2);
            Tuple3<String, Long, Long> kickedElem = Tuple3.of(null, 0L, 0L);

            if(u_v.f1 != null && u_v.f1 != -1 && layerPrefilter.getFromSlot(u_v.f0, u_v.f1).f0.equals(key)){
                if(j == topMostLayer)
                    newFreq = w+layers[topMostLayer].getTopKPrefilter().getFromSlot(u_v.f0, u_v.f1).f1;
                layerPrefilter.updateFilter(key, u_v.f0, u_v.f1, newFreq);
                oldBottomLayer = j;
            }else if(u_v.f1 != null && u_v.f1 == -1){
                if(j == topMostLayer)
                    newFreq = w;
                kickedElem = layerPrefilter.insertFilter(u_v.f0, key, newFreq, 0);
            }else{
                if(j == topMostLayer) {
                    layers[topMostLayer].getSketch().add(key, w);
                    newFreq = layers[topMostLayer].getSketch().estimateCount(key);
                }

                if(newFreq <= layerPrefilter.getApproxGlobalMin())
                    break;
                kickedElem = layerPrefilter.insertFilter(u_v.f0, key, newFreq, newFreq);
            }

            if(kickedElem.f0 != null){
                if(kickedElem.f0.equals(key))
                    break;
                int kickedTopmostLayer = getTopMostLayer(hashFunc(kickedElem.f0));
                if(kickedTopmostLayer == j)
                    layers[kickedTopmostLayer].getSketch().add(kickedElem.f0, kickedElem.f1-kickedElem.f2);
                boolean isSampledAbove = (j+1) <= kickedTopmostLayer;
                momentEstimator.kickedElemRm(j, kickedElem.f1, isSampledAbove);
            }

            newBottomLayer = j;
        }

        boolean existsAfter = newBottomLayer <= topMostLayer;
        boolean existsBefore = oldBottomLayer <= topMostLayer;
        momentEstimator.update(oldBottomLayer, newFreq-w, newBottomLayer, newFreq, existsAfter, existsBefore);
    }

    private long mix(long x){
        x ^= x >>> 16;
        x *= 0x85ebca6bL;
        x ^= x >>> 13;
        x *= 0xc2b2ae35L;
        x ^= x >>> 16;
        return x;
    }

    public RAS getRas(int level){
        return layers[level];
    }

    public MomentEstimator getMomentEstimator() {
        return momentEstimator;
    }

    public Map<String, Long> getTopKElements(int option, double epsilonThreshold){
        double threshold = epsilonThreshold*getStreamMomentEstimation(option);
        Map<String, Long> topElems = new HashMap<>();

        for(int l = totalLayers-1; l >= 0; l--)
            for(Tuple3<String, Long, Long> entry : layers[l].getTopElements())
                if(momentEstimator.g(entry.f1, option) >= threshold)
                    topElems.putIfAbsent(entry.f0, entry.f1);

        return topElems;
    }

    public double getStreamMomentEstimation(int option){
        if(!momentEstimator.isOnline()) {
            momentEstimator.clearOffline();

            for(int i = 0; i < totalLayers; i++){
                for(Tuple3<String, Long, Long> elem: layers[i].getTopElements()){
                    boolean isSampledAbove = ((i+1) < totalLayers) && ((i+1) <= getTopMostLayer(hashFunc(elem.f0)));
                    momentEstimator.offlineUpdate(i, elem.f1, isSampledAbove, option);
                }
            }
        }
        return momentEstimator.getStreamMomentEstimation(option);
    }

    public long getPerElementFreqEstimation(String key) {
        int topMostLayer = getTopMostLayer(hashFunc(key));
        return layers[topMostLayer].getPerElementFrequency(key);
    }

    public boolean merge(RUS rus) throws BufferOverflowException {
        if(rus != null){
            if(this.totalLayers != rus.getTotalLayers())
                return false;
            for(RAS layer : rus.getLayers()){
                if(layer == null)
                    return false;
            }
            for(int i = 0; i < totalLayers; i++){
                if(!this.layers[i].canMerge(rus.getRas(i)))
                    return false;
            }
            if(rus.getMomentEstimator() == null)
                return false;

            if(!this.momentEstimator.canMerge(rus.getMomentEstimator()))
                return false;

            this.momentEstimator.merge(rus.getMomentEstimator());

            for(int i = totalLayers-1; i >= 0; i--) {
                ArrayList<Tuple3<String, Long, Long>> kickedElems = this.layers[i].merge(rus.getRas(i));
                if(kickedElems == null)
                    return false;


                for(Tuple3<String, Long, Long> elem : kickedElems) {
                    if(momentEstimator.isOnline()) {
                        int topMostLayer = getTopMostLayer(hashFunc(elem.f0));
                        boolean sampledAbove = ((i + 1) <= topMostLayer);
                        this.momentEstimator.kickedElemRm(i, elem.f1, sampledAbove);
                    }
                    this.layers[i].getSketch().add(elem.f0, elem.f1-elem.f2);
                }
            }

            return true;
        }

        return false;
    }

    public int getTotalLayers() {
        return totalLayers;
    }

    public RAS[] getLayers() {
        return layers;
    }

    public void setTotalLayers(int totalLayers) {
        this.totalLayers = totalLayers;
    }

    public void setLayers(RAS[] layers) {
        this.layers = layers;
    }

    public void setMomentEstimator(MomentEstimator momentEstimator) {
        this.momentEstimator = momentEstimator;
    }
}
