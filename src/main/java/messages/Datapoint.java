package messages;

import java.io.Serializable;


public class Datapoint implements Serializable {
    private String key;
    private int freq;

    public Datapoint(){}

    public Datapoint(String key, int freq){
        this.key = key;
        this.freq = freq;
    }

    @Override
    public String toString() {
        return "Datapoint{" +
                "key='" + key + '\'' +
                ", freq=" + freq +
                '}';
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }
}
