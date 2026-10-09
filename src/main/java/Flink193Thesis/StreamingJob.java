/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Flink193Thesis;

import Transformations.EstimateReduce;
import Transformations.SynopsesMaintenance;
import com.fasterxml.jackson.databind.ObjectMapper;
import messages.Datapoint;
import messages.Estimation;
import messages.Request;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sources.KafkaEstimationProducer;
import sources.KafkaStringConsumer;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
    private static String kafkaDataInputTopic;
    private static String kafkaRequestInputTopic;
    private static String kafkaBrokersList;
    private static String kafkaRequestsGroup;
    private static String kafkaDataGroup;
    private static int parallelism;
    private static String kafkaOutputTopic;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static void initParams(String[] args){
        if(args.length > 4){
            kafkaDataInputTopic = args[0];
            kafkaRequestInputTopic = args[1];
            kafkaOutputTopic = args[2];
            kafkaBrokersList = args[3];
            parallelism = Integer.parseInt(args[4]);
            kafkaDataGroup = args[5];
            kafkaRequestsGroup = args[6];
            System.out.printf("Starting job with params [%s, %s, %s, %s, %d]\n",  kafkaDataInputTopic, kafkaRequestInputTopic, kafkaOutputTopic,kafkaBrokersList, parallelism);
        }else if(args.length == 1){
            System.out.println("Starting job with default parameters and parallelism: ["+args[0]+"]\n");
            kafkaDataInputTopic = "data_ingTest_vkouts";
            kafkaRequestInputTopic = "requests_vkouts";
            kafkaOutputTopic = "estimations_vkouts";
            kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            parallelism = Integer.parseInt(args[0]);
            kafkaDataGroup = "data_consumer_group";
            kafkaRequestsGroup = "requests_consumer_group";
        }
    }

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
        initParams(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(parallelism);

        KafkaStringConsumer dataSource= new KafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic, kafkaDataGroup, true);
        KafkaStringConsumer requestsSource = new KafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic, kafkaRequestsGroup, false);
        KafkaEstimationProducer estimationsSink = new KafkaEstimationProducer(kafkaBrokersList, kafkaOutputTopic);


        DataStream<String> data = env.addSource(dataSource.getSource()).name("DATA_SOURCE");
        DataStream<String> requests = env.addSource(requestsSource.getSource()).name("REQUEST_SOURCE");

        DataStream<Datapoint> dataPointStream = data.map(new MapFunction<String, Datapoint>() {
            @Override
            public Datapoint map(String node) throws Exception {
                return MAPPER.readValue(node, Datapoint.class);
            }
        }).name("DATA_MAP").keyBy(Datapoint::getKey);

        DataStream<Request> requestStream = requests.map(new MapFunction<String, Request>() {
            @Override
            public Request map(String node) throws Exception {
                return MAPPER.readValue(node, Request.class);
            }
        }).name("REQUEST_MAP");

        MapStateDescriptor<Integer, Request> mDesc = new  MapStateDescriptor<>("requests", Integer.class, Request.class);
        BroadcastStream<Request> requestsBroadcasts = requestStream.broadcast(mDesc);

        DataStream<Estimation> estimationStream = dataPointStream.keyBy(Datapoint::getKey)
                .connect(requestsBroadcasts)
                .process(new SynopsesMaintenance()).name("SYNOPSES_MAINTENANCE");

        DataStream<Estimation> finalStream = estimationStream.keyBy(Estimation::getqUID)
                .process(new EstimateReduce()).name("REDUCE");

        finalStream.addSink(estimationsSink.getSink()).name("ESTIMATIONS_SINK");
        // execute program
		env.execute("RUS Streaming vkoutsovasilis");
	}


}
