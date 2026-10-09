package sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import messages.Estimation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;

public class KafkaEstimationProducer implements Serializable {
    private FlinkKafkaProducer<Estimation> sink;

    public KafkaEstimationProducer(){}

    public KafkaEstimationProducer(String brokerList, String topic) {

        sink = new FlinkKafkaProducer<>(
                brokerList,
                topic,
                new EstimationRecordSerializer(topic)
        );
        sink.setWriteTimestampToKafka(true);
    }

    public FlinkKafkaProducer<Estimation> getSink() {
        return sink;
    }

    public void setSink(FlinkKafkaProducer<Estimation> sink) {
        this.sink = sink;
    }
}

class EstimationRecordSerializer implements KeyedSerializationSchema<Estimation>, Serializable {
    public final String topic;

    public EstimationRecordSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public byte[] serializeKey(Estimation estimation) {
        return (""+estimation.getRequestID()).getBytes();
    }

    @Override
    public byte[] serializeValue(Estimation estimation) {
        try{
            return estimation.toKafkaJson();
        }catch(JsonProcessingException e){
            e.printStackTrace();
        }
        return estimation.toKafka();
    }

    @Override
    public String getTargetTopic(Estimation estimation) {
        return this.topic;
    }
}
