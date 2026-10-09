package UniversalSketch;

import java.io.Serializable;

public class MomentEstimator implements Serializable {
    private final int totalOptions = 5;
    private boolean online;
    private double[] layer0Estimations;
    private double offlineEstimation;

    public MomentEstimator(){
        this.layer0Estimations = new double[totalOptions];
        this.online = true;
    }

    public MomentEstimator(boolean online){
        this.layer0Estimations = new double[totalOptions];
        this.online = online;
        if(!online)
            this.offlineEstimation = 0.0;
    }

    /**
     *  Function to apply on a given frequency to get a moment estimation
     * @param f The given frequency
     * @param option The order of the moment:
     *               <ul>
     *                   <li>0: L0: f(x) = 1</li>
     *                   <li>1: L1: f(x) = x</li>
     *                   <li>2: L2: f(x) = x<sup>2</sup></li>
     *                   <li>3: L3: f(x) = x<sup>3</sup></li>
     *                   <li>4: Entropy: f(x) = x*log<sub>2</sub>(x)</li>
     *               <ul/>
     *
     * @return The moment estimation for this frequency
     */
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

    public void update(int oldBL, long oldFreq, int newBL, long newFreq, boolean existsAfter, boolean existsBefore){
        if(!online)
            return;

        for(int opt = 0; opt < totalOptions; opt++)
            layer0Estimations[opt] += (existsAfter ? 1 : 0)*(1L << newBL)*g(newFreq, opt)-(existsBefore ? 1 : 0)*(1L << oldBL)*g(oldFreq, opt);


    }

    public void offlineUpdate(int layer, long freq, boolean sampled, int option){
        offlineEstimation += (1L << layer)*(1-2*(sampled ? 1 : 0)) * g(freq, option);
    }

    public void kickedElemRm(int layer, long freq, boolean sampled){
        if(!online)
            return;

        double layerContribution = (1L << layer) * (1 - 2 * (sampled ? 1 : 0));

        for(int opt = 0; opt < totalOptions; opt++)
            layer0Estimations[opt] -= layerContribution * g(freq, opt);
    }

    public double[] getLayer0Estimations() {
        return layer0Estimations;
    }

    public int getTotalOptions(){
        return totalOptions;
    }

    public double getStreamMomentEstimation(int option){
        return  (online) ? layer0Estimations[option] : offlineEstimation;
    }

    public void clearOffline(){
        offlineEstimation = 0.0;
    }

    public boolean canMerge(MomentEstimator other){
        if(other != null){
            if(this.totalOptions != other.getTotalOptions())
                return false;
            return this.online == other.isOnline();
        }
        return false;
    }

    public void merge(MomentEstimator me){
        if(online)
            for(int opt = 0; opt < this.totalOptions; opt++)
                this.layer0Estimations[opt] += me.getStreamMomentEstimation(opt);
        else
            this.offlineEstimation += me.offlineEstimation;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    public void setLayer0Estimations(double[] layer0Estimations) {
        this.layer0Estimations = layer0Estimations;
    }

    public void setOfflineEstimation(double offlineEstimation) {
        this.offlineEstimation = offlineEstimation;
    }

    public double getOfflineEstimation() {
        return offlineEstimation;
    }
}
