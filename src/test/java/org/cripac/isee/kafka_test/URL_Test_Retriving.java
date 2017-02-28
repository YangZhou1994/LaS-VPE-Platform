package org.cripac.isee.kafka_test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.relaxng.datatype.Datatype;

import java.io.IOException;
import java.io.SyncFailedException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;

/**
 * Created by yang on 17-2-27.
 */
public class URL_Test_Retriving {

    final static Stream.Port TEST_URL_SAVE_RETRIVE_PORT =
            new Stream.Port("topicForURLTest", DataType.URL);

    public static void main(String[] args){
        KafkaConsumer<String, byte[]> consumer;
        ConsoleLogger logger;
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ktask-nod1:9092,ktask-nod2:9092,ktask-nod3:9092,ktask-nod4:9092");
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG,"testGroup");
        consumerProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class);
        consumerProp.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);

        consumer = new KafkaConsumer<>(consumerProp);
        consumer.subscribe(Collections.singletonList(TEST_URL_SAVE_RETRIVE_PORT.name));
        ConsumerRecords<String, byte[]> records;

        Kafka_Url_Test URLTest = new Kafka_Url_Test();

        ArrayList<Tracklet> testSample = new ArrayList<>();
        //Receiving URL from Kafka
        long startTime = System.currentTimeMillis();
        System.out.printf("Consumer is already waiting for Kafka messages at: %d ms !",startTime);
        System.out.println();

        //Waiting for Response
        while (testSample.size() < 1000 ) {

                records = consumer.poll(0);
                if (records.isEmpty()) {
                    continue;
                }

                records.forEach((ConsumerRecord<String, byte[]> rec) -> {
                    TaskData taskData;
                    try {
                        taskData = deserialize(rec.value());
                    } catch (Exception e) {
                        System.out.println("Error happened during Deserialize!");
                        e.printStackTrace();
                        return;
                    }
                    if (taskData.destPorts.containsKey(TEST_URL_SAVE_RETRIVE_PORT)) {
                       String trackletURL = (String) taskData.predecessorRes;
                        try {
                            assert trackletURL != null;
                            testSample.add(Kafka_Url_Test.testTrackletsRetrieving(trackletURL));
                            //System.out.printf("Totally got %d URL",testSample.size());
                            //System.out.println();
                        } catch (URISyntaxException | IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("100 Tracklets have been read from HDFS Successfully at: %d ms",endTime);
        System.out.println();
        System.out.println();
        System.out.println();
        //System.out.println(testSample.get(99));
    }
}
