package UniversalSketch;

import org.apache.commons.collections.BufferOverflowException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

public class RA_SuCS implements Serializable {
    private int width, d, seed;
    private double epsilon, confidence;
    private Random updateRand;

    private RAC[] counters;
    private long[] hashA, hashB, signA, signB;

    public static final int HL = 31;
    public static final long MOD = (1L << HL) - 1;

    public RA_SuCS() {
        this.epsilon = 0.01;
        this.confidence = 0.99;
        this.width = 8*(int)Math.ceil(3/(epsilon*epsilon));
        this.d = (int)Math.ceil(Math.log(1/(1-confidence)));
        this.seed = 12345;
        initializeCounters(seed);
    }

    public RA_SuCS(int width, int d, int seed) {
        this.width = width;
        this.d = d;
        this.epsilon = Math.sqrt(24/(double)width);
        this.confidence = 1 - Math.exp(-d);
        this.seed = seed;
        initializeCounters(seed);
    }

    public RA_SuCS(double epsilon, double confidence, int seed){
        this.epsilon = epsilon;
        this.confidence = confidence;
        this.width = 8*(int)Math.ceil(3/(epsilon*epsilon));
        this.d = (int)Math.ceil(Math.log(1/(1-confidence)));
        this.seed = seed;
        initializeCounters(seed);
    }

    public RA_SuCS(double epsilon, int d, int seed){
        this.epsilon = epsilon;
        this.confidence = 1-Math.exp(-d);
        this.d = d;
        this.width = 8*(int)Math.ceil(3/(epsilon*epsilon));
        this.seed = seed;
        initializeCounters(seed);
    }

    private void initializeCounters(int seed){
        this.counters = new RAC[width];

        this.hashA = new long[d];
        this.hashB = new long[d];

        this.signA = new long[d];
        this.signB = new long[d];

        Random hRand = new Random(mix(seed));
        Random sRand = new Random(mix(seed+1));
        updateRand = new Random(mix(seed+2));

        for(int i = 0; i < d; i++){
            hashA[i] = hRand.nextInt(Integer.MAX_VALUE-1)+1L;
            hashB[i] = hRand.nextInt(Integer.MAX_VALUE);

            signA[i] = sRand.nextInt(Integer.MAX_VALUE-1)+1L;
            signB[i] = sRand.nextInt(Integer.MAX_VALUE);
        }

        for(int j = 0; j < width; j++){
            this.counters[j] = new RAC((int)mix(seed+j*1000L));
        }
    }

    private long hash31(long a, long b, long x){
        long result = (a * x) + b;
        long sum =  ((result >> HL) + result) & MOD;
        if(sum >= 0x7FFFFFFF) sum -= 0x7FFFFFFF;
        return sum;
    }

    private long mix(long x){
        x ^= x >>> 16;
        x *= 0x85ebca6bL;
        x ^= x >>> 13;
        x *= 0xc2b2ae35L;
        x ^= x >>> 16;
        return x;
    }

    public void add(long key, long w) throws BufferOverflowException {
        long maskedKey = key & MOD;
        int[] buckets = new int[d];
        int[] signs = new int[d];

        for(int i=0; i < d; i++){
            buckets[i] = (int) (hash31(hashA[i], hashB[i], maskedKey) % this.width);
            long signHash = hash31(signA[i], signB[i], maskedKey);
            signs[i] = ((signHash & 1) == 1) ? 1 : -1;
        }

        int randChoice = updateRand.nextInt(d);
        this.counters[buckets[randChoice]].add((int)(signs[randChoice]*w));
    }

    public void add(String key, long w) throws BufferOverflowException{
        add(key.hashCode() & 0x7FFFFFFF, w);
    }

    public long estimateCount(long key){
        long maskedKey = key & MOD;
        long result = 0;

        for(int i = 0; i < d; i++){
            int bucket = (int) (hash31(hashA[i], hashB[i], maskedKey) % this.width);
            long signHash = hash31(signA[i], signB[i], maskedKey);
            int sign = ((signHash & 1L) == 1L) ? 1 : -1;

            result += sign*counters[bucket].get_value();
        }

        return (result < 0) ?  1 : result;
    }

    public long estimateCount(String key){
        return estimateCount(key.hashCode() & 0x7FFFFFFF);
    }

    public boolean canMerge(RA_SuCS other){
        if(other != null){
            if(this.width != other.getWidth())
                return false;
            if(this.d != other.getD())
                return false;
            return this.seed == other.getSeed();
        }
        return false;
    }

    public void merge(RA_SuCS estimator) throws BufferOverflowException{
        for(int i = 0; i < this.width; i++)
            this.counters[i].add(estimator.getCounters()[i].get_value());
    }

    @Override
    public String toString() {
        return "RA_SuCS{" +
                "confidence=" + confidence +
                ", epsilon=" + epsilon +
                ", d=" + d +
                ", width=" + width +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        final RA_SuCS other = (RA_SuCS) obj;

        if (this.width != other.getWidth())
            return false;
        if (this.d != other.getD())
            return false;

        if (Double.compare(this.epsilon, other.getEpsilon()) != 0)
            return false;
        if (Double.compare(this.confidence, other.getConfidence()) != 0)
            return false;
        if (!Arrays.equals(this.hashA, other.getHashA()))
            return false;
        if (!Arrays.equals(this.hashB, other.getHashB()))
            return false;
        if (!Arrays.equals(this.signA, other.getSignA()))
            return false;
        if (!Arrays.equals(this.signB, other.getSignB()))
            return false;

        return Arrays.deepEquals(this.counters, other.getCounters());
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getD() {
        return d;
    }

    public void setD(int d) {
        this.d = d;
    }

    public int getSeed() {
        return seed;
    }

    public void setSeed(int seed) {
        this.seed = seed;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public RAC[] getCounters() {
        return counters;
    }

    public void setCounters(RAC[] counters) {
        this.counters = counters;
    }

    public long[] getHashA() {
        return hashA;
    }

    public void setHashA(long[] hashA) {
        this.hashA = hashA;
    }

    public long[] getHashB() {
        return hashB;
    }

    public void setHashB(long[] hashB) {
        this.hashB = hashB;
    }

    public long[] getSignA() {
        return signA;
    }

    public void setSignA(long[] signA) {
        this.signA = signA;
    }

    public long[] getSignB() {
        return signB;
    }

    public void setSignB(long[] signB) {
        this.signB = signB;
    }

    public Random getUpdateRand() {
        return updateRand;
    }

    public void setUpdateRand(Random updateRand) {
        this.updateRand = updateRand;
    }
}