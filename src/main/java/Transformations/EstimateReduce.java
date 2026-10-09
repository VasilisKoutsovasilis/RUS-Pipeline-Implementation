package Transformations;

import messages.Estimation;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EstimateReduce extends KeyedProcessFunction<String, Estimation, Estimation> {
    private transient ListState<Map<String, Long>> topKPartialEstimates;
    private transient ValueState<Double> accumulatedVal;
    private transient ValueState<Integer> arrivalCount;
    private transient ValueState<Long> cleanupTimerState;
    public static final long TIMEOUT_MS = 10000L;
    public long topKComputedMS = -1L;
    public long pointQueryComputedMs =-1L;

    @Override
    public void open(Configuration parameters) throws Exception {
        topKPartialEstimates = getRuntimeContext().getListState(
                new ListStateDescriptor<>("estimations", TypeInformation.of(new TypeHint<Map<String, Long>>() {}))
        );
        arrivalCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalPEstimates", Integer.class)
        );
        accumulatedVal = getRuntimeContext().getState(
                new ValueStateDescriptor<>("freqOrMomentEstimate", Double.class)
        );
        cleanupTimerState =  getRuntimeContext().getState(
                new ValueStateDescriptor<>("cleanupTimerState", Long.class)
        );
        getRuntimeContext()
                .getMetricGroup()
                .gauge("topKComputedMS", ()->topKComputedMS);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("pointQueryComputedMS", ()->pointQueryComputedMs);
    }

    @Override
    public void processElement(Estimation estimation,
                               KeyedProcessFunction<String, Estimation, Estimation>.Context context,
                               Collector<Estimation> collector) throws Exception {
        int count = arrivalCount.value() == null ? 1 : (arrivalCount.value() + 1);
        arrivalCount.update(count);
        if(count == 1){
            long cleanupTime = context.timerService().currentProcessingTime() + TIMEOUT_MS;
            context.timerService().registerProcessingTimeTimer(cleanupTime);
            cleanupTimerState.update(cleanupTime);
        }

        boolean isPointQuery = estimation.getParams()[2].isEmpty();
        if(isPointQuery){
            double currSum = accumulatedVal.value() == null ? 0.0 : accumulatedVal.value();
            accumulatedVal.update(currSum + (Double)estimation.getEstimation());

            if(count == estimation.getParallelism()){
                pointQueryComputedMs = System.currentTimeMillis();
                Long timestamp = cleanupTimerState.value();
                if(timestamp != null)
                    context.timerService().deleteProcessingTimeTimer(timestamp);

                collector.collect(new Estimation(
                        accumulatedVal.value(),
                        estimation.getRequestID(),
                        estimation.getSynopsisID(),
                        estimation.getParallelism(),
                        estimation.getParams(),
                        estimation.getqUID()));
                clearState();
            }
        }else{
            Tuple2<Map<String, Long>, Double> partialEst = (Tuple2<Map<String, Long>, Double>) estimation.getEstimation();
            double currThresh = accumulatedVal.value() == null? 0.0 : accumulatedVal.value();
            double epsilon = Double.parseDouble(estimation.getParams()[2]);
            accumulatedVal.update(currThresh+epsilon*partialEst.f1);
            topKPartialEstimates.add(partialEst.f0);
            int option = Integer.parseInt(estimation.getParams()[1]);

            if(count == estimation.getParallelism()){
                Long timestamp = cleanupTimerState.value();
                if(timestamp != null)
                    context.timerService().deleteProcessingTimeTimer(timestamp);

                double threshold = accumulatedVal.value();
                List<String> result = new ArrayList<>();
                Map<String, Long> globalTopK = new HashMap<>();

                for(Map<String, Long> partialTopKs: topKPartialEstimates.get())
                    for(Map.Entry<String, Long> entry: partialTopKs.entrySet())
                        globalTopK.merge(entry.getKey(), entry.getValue(), Long::sum);

                for(Map.Entry<String, Long> entry: globalTopK.entrySet())
                    if(g(entry.getValue(), option) >= threshold)
                        result.add(entry.getKey()+"/"+entry.getValue());

                topKComputedMS = System.currentTimeMillis();
                collector.collect(new Estimation(
                        result.toString(),
                        estimation.getRequestID(),
                        estimation.getSynopsisID(),
                        estimation.getParallelism(),
                        estimation.getParams(),
                        estimation.getqUID()));
                clearState();
            }
        }
    }

    private void clearState() throws Exception {
        arrivalCount.clear();
        accumulatedVal.clear();
        topKPartialEstimates.clear();
        cleanupTimerState.clear();
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Estimation, Estimation>.OnTimerContext ctx, Collector<Estimation> out) throws Exception {
        Long timerTimestamp = cleanupTimerState.value();
        if(timerTimestamp != null && timestamp == timerTimestamp)
            clearState();
    }

    public double g(double f, int option){
        if(f <= 0) return 0;

        switch (option){
            case 0: return 1;
            case 1: return f;
            case 2: return f*f;
            case 3: return Math.pow(f, 3);
            case 4: return f*(Math.log(f)/Math.log(2));
            default: return -1;
        }
    }
}
