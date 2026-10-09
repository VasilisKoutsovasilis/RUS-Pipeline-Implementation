package UniversalSketch;

import messages.Datapoint;
import messages.Estimation;
import messages.Request;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

public class RUS_Synopsis extends Synopsis {
    private RUS rus;

    public RUS_Synopsis() {
        super(1);
    }

    public RUS_Synopsis(int uid, String[] parameters){
        super(uid);
        int availableMem = Integer.parseInt(parameters[0])*((Boolean.parseBoolean(parameters[1])) ? 1024 : 1024*1024);
        int k =  Integer.parseInt(parameters[2]);
        int layers = Integer.parseInt(parameters[3]);
        boolean isOnline = Boolean.parseBoolean(parameters[4]);
        int seed = Integer.parseInt(parameters[5]);

        int memForSketches = Math.max(64*1024, availableMem - layers*k*(4+8+8));
        int slotSize = 8;
        int rows = k/slotSize;
        int sketchWidth = memForSketches/(layers*2);
        this.rus = new RUS(layers, rows, slotSize, sketchWidth, 1, false, isOnline, seed);
    }

    @Override
    public void add(Object k) {
        Datapoint dp = (Datapoint) k;
        rus.add(dp.getKey(), dp.getFreq());
    }

    @Override
    public Object estimate(Object k) {
        return rus.getPerElementFreqEstimation((String)k);
    }

    @Override
    public Estimation estimate(Request rq) {
        String key =  rq.getParams()[0];
        String option = rq.getParams()[1];
        String epsilon = rq.getParams()[2];

        if(key.isEmpty()){
            if(epsilon.isEmpty())
                return new Estimation(rus.getStreamMomentEstimation(Integer.parseInt(option)),
                        rq.getRequestID(), rq.getSketchID(), rq.getParallelism(), rq.getParams(), rq.getqUID());
            else {
                Tuple2<Map<String, Long>, Double> est = Tuple2.of(rus.getTopKElements(Integer.parseInt(option),
                        Double.parseDouble(epsilon)),rus.getStreamMomentEstimation(Integer.parseInt(option)));
                return new Estimation(est, rq.getRequestID(), rq.getSketchID(), rq.getParallelism(), rq.getParams(), rq.getqUID());
            }
        }else
            return new Estimation((double)rus.getPerElementFreqEstimation(key),
                    rq.getRequestID(), rq.getSketchID(), rq.getParallelism(), rq.getParams(), rq.getqUID());
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        if(sk != null){
            RUS_Synopsis rsk = (RUS_Synopsis)sk;
            if(rsk.getRus().merge(this.rus))
                return rsk;
        }
        return null;
    }

    public RUS getRus() {
        return rus;
    }

    public void setRus(RUS rus) {
        this.rus = rus;
    }
}
