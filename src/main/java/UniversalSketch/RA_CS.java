package UniversalSketch;

import org.apache.commons.collections.BufferOverflowException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

public class RA_CS implements Serializable {
    private int width, depth, seed;
    private double epsilon, confidence;

    private RAC[][] counters;
    private long[] hashA, hashB, signA, signB;

    public static final int HL = 31;
    public static final long MOD = (1L << HL) - 1;

    public RA_CS() {
        this.epsilon = 0.01;
        this.confidence = 0.99;
        this.width = 2*(int)Math.ceil(3/epsilon);
        this.depth = 2*(int)Math.ceil(Math.log(1/(1-confidence)));
        this.seed = 12345;
        initializeCounters(seed);
    }

    public RA_CS(int width, int depth, int seed){
        this.width = width;
        this.depth = depth;
        this.epsilon = Math.ceil(Math.sqrt(6/(double)width));
        this.confidence = 1-Math.exp(-depth);
        this.seed = seed;
        initializeCounters(seed);
    }

    public RA_CS(double epsilon, double confidence, int seed){
        this.epsilon = epsilon;
        this.confidence = confidence;
        this.width = 2*(int)Math.ceil(3/(epsilon*epsilon));
        this.depth = (int)Math.ceil(Math.log(1/(1-confidence)));
        this.seed = seed;
        initializeCounters(seed);
    }

    public RA_CS(double epsilon, double confidence, int universeSize, int seed){
        this.epsilon = epsilon;
        this.confidence = confidence;
        this.width = 2*(int)Math.ceil(3/(epsilon*epsilon));
        this.depth = (int)Math.ceil(Math.log(universeSize/(1-confidence)));
        this.seed = seed;
        initializeCounters(seed);
    }

    public RA_CS(double epsilon, int depth, int seed){
        this.epsilon = epsilon;
        this.confidence = 1- Math.exp(-depth);
        this.depth = depth;
        this.width = 2*(int)Math.ceil(3/(epsilon*epsilon));
        this.seed = seed;
        initializeCounters(seed);
    }

    private long mix(long x){
        x ^= x >>> 16;
        x *= 0x85ebca6bL;
        x ^= x >>> 13;
        x *= 0xc2b2ae35L;
        x ^= x >>> 16;
        return x;
    }

    private void initializeCounters(int seed){
        this.counters = new RAC[depth][width];

        this.hashA = new long[depth];
        this.hashB = new long[depth];

        this.signA = new long[depth];
        this.signB = new long[depth];

        Random hRand = new Random(mix(seed));
        Random sRand = new Random(mix(seed+1));

        for(int i = 0; i < depth; i++){
            hashA[i] = hRand.nextInt(Integer.MAX_VALUE-1)+1L;
            hashB[i] = hRand.nextInt(Integer.MAX_VALUE);

            signA[i] = sRand.nextInt(Integer.MAX_VALUE-1)+1L;
            signB[i] = sRand.nextInt(Integer.MAX_VALUE);
            for(int j = 0; j < width; j++)
                this.counters[i][j] = new RAC((int)mix(seed+j));
        }
    }

    private long hash31(long a, long b, long x){
        long result = (a * x) + b;
        long sum =  ((result >> HL) + result) & MOD;
        if(sum >= 0x7FFFFFFF) sum -= 0x7FFFFFFF;
        return sum;
    }

    public void add(long key, long w) throws BufferOverflowException {
        long maskedKey = key & MOD;

        for(int i = 0; i < depth; i++){
            int bucket = (int) (hash31(hashA[i], hashB[i], maskedKey) % this.width);
            long signHash = hash31(signA[i], signB[i], maskedKey);
            int sign = ((signHash & 1) == 1) ? 1 : -1;

            counters[i][bucket].add((int)(sign*w));
        }
    }

    public void add(String key, long w) throws BufferOverflowException{
        add(key.hashCode() & 0x7FFFFFFF, w);
    }

    /**
     * Method to estimate the count of an element with a given key. Gathers the count of each counter
     * from d levels and calculates the median value representing the estimated count.
     * @param key the key of the element to estimate the count
     * @return the median value representing the count of element with given key.
     */
    public int estimateCount(long key){
        long maskedKey = key & MOD;
        int[] estimates = new  int[depth];

        for(int i = 0; i < depth; i++){
            int bucket = (int) (hash31(hashA[i], hashB[i], maskedKey) % this.width);
            long signHash =  hash31(signA[i], signB[i], maskedKey);
            int sign = ((signHash & 1) == 1) ? 1 : -1;

            estimates[i] = counters[i][bucket].get_value()*sign;
        }

        Arrays.sort(estimates);
        return (depth % 2 == 1) ? estimates[depth/2] : (estimates[depth/2]+estimates[depth/2-1])/2;
    }

    public int estimateCount(String key){
        return estimateCount(key.hashCode() & 0x7FFFFFFF);
    }

    public boolean canMerge(RA_CS other){
        if(other != null){
            if(this.depth != other.getDepth())
                return false;
            if(this.width != other.getWidth())
                return false;
            return this.seed == other.getSeed();
        }
        return false;
    }

    /**
     *  Method to merge two Count Sketches with RAC counters
     * @param estimator the RA_CS to merge with
     */
    public void merge(RA_CS estimator) throws BufferOverflowException{
        for(int d = 0; d < this.depth; d++)
            for(int w = 0; w < this.width; w++)
                this.counters[d][w].add(estimator.getCounters()[d][w].get_value());
    }

    @Override
    public String toString() {
        return "RA_CS{" +
                "confidence=" + confidence +
                ", epsilon=" + epsilon +
                ", depth=" + depth +
                ", width=" + width +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        final RA_CS other = (RA_CS) obj;

        if (this.width != other.getWidth())
            return false;
        if (this.depth != other.getDepth())
            return false;

        if (Double.compare(this.epsilon, other.getEpsilon()) != 0)
            return false;
        if (Double.compare(this.confidence, other.getConfidence()) != 0)
            return false;
        if(!Arrays.equals(this.hashA, other.getHashA()))
            return false;
        if(!Arrays.equals(this.hashB, other.getHashB()))
            return false;
        if(!Arrays.equals(this.signB, other.getSignB()))
            return false;
        if(!Arrays.equals(this.signA, other.getSignA()))
            return false;

        return Arrays.deepEquals(this.counters, other.getCounters());
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
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

    public RAC[][] getCounters() {
        return counters;
    }

    public void setCounters(RAC[][] counters) {
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
}
