package messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Arrays;

public class Estimation implements Serializable {
    private Object estimation;
    private int requestID;
    private int synopsisID;
    private int parallelism;
    private String[] params;
    private String qUID;

    public Estimation(){}

    public Estimation(Object estimation, int requestID, int synopsisID, int parallelism, String[] params, String qUID) {
        this.estimation = estimation;
        this.requestID = requestID;
        this.synopsisID = synopsisID;
        this.parallelism = parallelism;
        this.params = params;
        this.qUID = qUID;
    }

    @Override
    public String toString() {
        return "Estimation{" +
                "estimation=" + estimation +
                ", requestID=" + requestID +
                ", synopsisID=" + synopsisID +
                ", parallelism=" + parallelism +
                ", params=" + Arrays.toString(params) +
                ", qUID='" + qUID + '\'' +
                '}';
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }

    public byte[] toKafkaJson() throws JsonProcessingException {
        return toJsonString().getBytes();
    }

    public byte[] toKafka() {
        String par = Arrays.toString(params).replace(",", ";");
        par = par.substring(1, par.length()-1).replaceAll("\\s+","");
        return ("\""+requestID+","+synopsisID+","+estimation+","+parallelism+","+par+","+qUID+"\"").getBytes();
    }

    public Object getEstimation() {
        return estimation;
    }

    public void setEstimation(Object estimation) {
        this.estimation = estimation;
    }

    public int getRequestID() {
        return requestID;
    }

    public void setRequestID(int requestID) {
        this.requestID = requestID;
    }

    public int getSynopsisID() {
        return synopsisID;
    }

    public void setSynopsisID(int synopsisID) {
        this.synopsisID = synopsisID;
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
