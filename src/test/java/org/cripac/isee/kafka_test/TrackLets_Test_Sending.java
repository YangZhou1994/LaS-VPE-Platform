package org.cripac.isee.kafka_test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.tracking.TrackletOrURL;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.cripac.isee.kafka_test.TrackLets_Test_Receiving.TEST_Tracklets_SAVE_RETRIVE_PORT;
import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * Created by yang on 17-3-1.
 */
public class TrackLets_Test_Sending {
    public static void main(String[] args) throws Exception {
        KafkaProducer<String, byte[]> producer;
        ConsoleLogger logger;

        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ktask-nod1:9092,ktask-nod2:9092,ktask-nod3:9092,ktask-nod4:9092");
        producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 100000000);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class);
        producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 100000000);
        producerProp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);

        producer = new KafkaProducer<>(producerProp);
        logger = new ConsoleLogger(Level.DEBUG);
/*
        final FileSystem hdfs;
        FileSystem tmpHDFS;
        while(true){
            try{
                tmpHDFS = new HDFSFactory().produce();
                break;
            }catch (IOException e){
                System.out.println("Fail to create hdfs connection.");

            }
        }
        hdfs = tmpHDFS;
*/
        //Tracking in real video.
        //Tracklet[] testTracklets= URLTest.testTracker("src/test/resources/20131220184349-20131220184937.h264");

        //Test with Fake Tracklets
        Tracklet[] testTracklets = new FakePedestrianTracker().track(null);
        //begin to counting time
        long startTime = System.currentTimeMillis();
        System.out.print("Start sending at: ");
        System.out.printf("%d ms",startTime);
        System.out.println();
        TaskData.ExecutionPlan executionPlan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node node = executionPlan.addNode(DataType.TRACKLET);
        Tracklet testTracklet = testTracklets[0];
        int i;
        for (i = 0 ; i < 100 ; ++i) {


            sendWithLog("topicForTrackletsSRTest","Tracklet_Deliver_"+i,
                    serialize(new TaskData(node.createInputPort(TEST_Tracklets_SAVE_RETRIVE_PORT),
                            executionPlan,new TrackletOrURL(testTracklet))),producer,logger);
            System.out.println();
            System.out.printf("%d",i);
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Finished saving %d tracklets at: %d ms",i+1,endTime);
        System.out.println();
        System.out.printf("The size of each serialized TaskData is %d bytes",(serialize(new TaskData(node.createInputPort(TEST_Tracklets_SAVE_RETRIVE_PORT),
                executionPlan,new TrackletOrURL(testTracklet)))).length);
        System.out.println();
        //hdfs.close();
        //tmpHDFS.close();


    }
}
