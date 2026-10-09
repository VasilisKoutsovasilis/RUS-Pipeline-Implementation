package sources;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.Serializable;
import java.util.Properties;

public class KafkaStringConsumer implements Serializable {
    private FlinkKafkaConsumer<String> source;

    public KafkaStringConsumer() {}

    public KafkaStringConsumer(String server, String topic, String consumerGroup, boolean startFromEarliest) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", server);
        props.setProperty("group.id", consumerGroup);
        if(startFromEarliest){
            props.setProperty("auto.offset.reset", "earliest");
            source = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props).setStartFromEarliest();
        }else{
            props.setProperty("auto.offset.reset", "latest");
            source = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props).setStartFromLatest();
        }

    }

    public FlinkKafkaConsumer<String> getSource() {
        return source;
    }

    public void setSource(FlinkKafkaConsumer<String> source) {
        this.source = source;
    }
}
