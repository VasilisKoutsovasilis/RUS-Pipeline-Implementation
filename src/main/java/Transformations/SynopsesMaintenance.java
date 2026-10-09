package Transformations;

import UniversalSketch.RUS_Synopsis;
import UniversalSketch.Synopsis;
import messages.Datapoint;
import messages.Estimation;
import messages.Request;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class SynopsesMaintenance extends KeyedBroadcastProcessFunction<Integer, Datapoint, Request, Estimation> {
    private transient Synopsis synopsis;
    public long firstRecordMs = -1L;
    public long lastRecordMs = -1L;
    public long topKReqArrivalMS = -1L;
    public long pointQueryArrivalMS = -1L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        synopsis = new RUS_Synopsis(1, new String[]{"768", "true", "2731", "12", "true", "42"});
        getRuntimeContext()
                .getMetricGroup()
                .gauge("firstRecordMs", ()->firstRecordMs);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("lastRecordMs", ()->lastRecordMs);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("topKReqArrivalMS", ()->topKReqArrivalMS);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("pointQueryArrivalMS", ()->pointQueryArrivalMS);
    }

    @Override
    public void processElement(Datapoint datapoint, KeyedBroadcastProcessFunction<Integer, Datapoint, Request, Estimation>.ReadOnlyContext readOnlyContext, Collector<Estimation> collector) throws Exception {

        if(synopsis != null) {
            if (firstRecordMs < 0)
                firstRecordMs = System.currentTimeMillis();
            synopsis.add(datapoint);
            lastRecordMs = System.currentTimeMillis();
        }
    }

    @Override
    public void processBroadcastElement(Request request, KeyedBroadcastProcessFunction<Integer, Datapoint, Request, Estimation>.Context context, Collector<Estimation> collector) throws Exception {
        switch(request.getRequestID()){
            case 1:
                if(synopsis == null){
                    if (request.getSketchID() == 1)
                        synopsis = new RUS_Synopsis(1, request.getParams());
                     else
                        synopsis = null;
                }
                break;
            case 2:
                if(synopsis != null)
                    synopsis = null;
                break;
            case 3:
                if(synopsis != null){
                    if(topKReqArrivalMS < 0 && !request.getParams()[2].isEmpty())
                        topKReqArrivalMS = System.currentTimeMillis();
                    else if(pointQueryArrivalMS < 0 && request.getParams()[2].isEmpty())
                        pointQueryArrivalMS = System.currentTimeMillis();
                    Estimation e = synopsis.estimate(request);
                    collector.collect(e);
                }
        }
    }
}
