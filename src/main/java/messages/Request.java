package messages;

import java.io.Serializable;
import java.util.Arrays;

public class Request implements Serializable{
    private int requestID;
    private int sketchID;
    private int parallelism;
    private String[] params;
    private String qUID;

    public Request(){}

    public Request(int requestID, int sketchID, int parallelism, String[] params, String qUID){
        this.requestID = requestID;
        this.sketchID = sketchID;
        this.parallelism = parallelism;
        this.params = params;
        this.qUID = qUID;
    }

    public Request(Request r){
        this.requestID = r.requestID;
        this.sketchID = r.sketchID;
        this.parallelism = r.parallelism;
        this.params = r.params;
        this.qUID = r.qUID;
    }

    @Override
    public String toString() {
        return "Request{" +
                "requestID=" + requestID +
                ", sketchID=" + sketchID +
                ", parallelism=" + parallelism +
                ", params=" + Arrays.toString(params) +
                ", qUID='" + qUID + '\'' +
                '}';
    }

    public int getRequestID() {
        return requestID;
    }

    public void setRequestID(int requestID) {
        this.requestID = requestID;
    }

    public int getSketchID() {
        return sketchID;
    }

    public void setSketchID(int sketchID) {
        this.sketchID = sketchID;
    }

    public String[] getParams() {
        return params;
    }

    public void setParams(String[] params) {
        this.params = params;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getqUID() {
        return qUID;
    }

    public void setqUID(String qUID) {
        this.qUID = qUID;
    }
}
