package org.cripac.isee.kafka_test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.tracking.TrackletOrURL;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;

/**
 * Created by yang on 17-3-1.
 */
public class TrackLets_Test_Receiving {
    final static Stream.Port TEST_Tracklets_SAVE_RETRIVE_PORT =
            new Stream.Port("topicForTrackletsSRTest", DataType.TRACKLET);

    public static void main(String[] args){
        KafkaConsumer<String, byte[]> consumer;
        ConsoleLogger logger;
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ktask-nod1:9092,ktask-nod2:9092,ktask-nod3:9092,ktask-nod4:9092");
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG,"testTrackGroup");
        consumerProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class);
        consumerProp.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        consumerProp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        consumer = new KafkaConsumer<>(consumerProp);
        consumer.subscribe(Collections.singletonList(TEST_Tracklets_SAVE_RETRIVE_PORT.name));
        ConsumerRecords<String, byte[]> records;

        ArrayList<Tracklet> testSample = new ArrayList<>();

        //Start receiving Tracklets from Kafka
        long startTime = System.currentTimeMillis();
        System.out.printf("Consumer is already waiting for Kafka messages at: %d ms !",startTime);
        System.out.println();

        //Waiting for Response
        while (testSample.size() < 1000 ) {

            records = consumer.poll(1000);
            if (records.isEmpty()) {
                continue;
            }

            records.forEach((ConsumerRecord<String, byte[]> rec) -> {
                TaskData taskData;
                String key;
                try {
                    taskData = deserialize(rec.value());
                    key = rec.key();
                } catch (Exception e) {
                    System.out.println("Error happened during Deserialize!");
                    e.printStackTrace();
                    return;
                }
                if (taskData.destPorts.containsKey(TEST_Tracklets_SAVE_RETRIVE_PORT)) {
                    try {
                        testSample.add(((TrackletOrURL) taskData.predecessorRes).getTracklet());
                        System.out.println(key);
                        System.out.printf("Totally got %d Tracklets",testSample.size());
                        System.out.println();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                System.gc();
            });
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("%d Tracklets have been read from Kafka Successfully at: %d ms",testSample.size(),endTime);
        System.out.println();
        System.out.println();
        System.out.println();
        //System.out.println(testSample.get(99));
    }
}
