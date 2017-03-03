package org.cripac.isee.kafka_test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;

import java.io.IOException;

import static org.cripac.isee.kafka_test.URL_Test_Retriving.TEST_URL_SAVE_RETRIVE_PORT;
import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import java.net.URI;
import java.util.Properties;
import java.util.UUID;

import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;


/**
 * Created by yang on 17-2-27.
 */
public class URL_Test_Saving {
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

        final FileSystem hdfs;
        FileSystem tmpHDFS;
        while(true){
            try{
                tmpHDFS = new HDFSFactory().produce();
                break;
            }catch (IOException e){
                System.out.println("Fail to create hdfs connection.");
                e.printStackTrace();
            }
        }
        hdfs = tmpHDFS;

        Kafka_Url_Test URLTest = new Kafka_Url_Test();

        Stream.Port VIDEO_URL_PORT =
                new Stream.Port("topicForURLTest", DataType.URL);

        //Tracking in real video.
        //Tracklet[] testTracklets= URLTest.testTracker("src/test/resources/20131220184349-20131220184937.h264");

        //Test with Fake Tracklets
        Tracklet[] testTracklets = new FakePedestrianTracker().track(null);
        //begin to counting time
        long startTime = System.currentTimeMillis();
        System.out.print("Start saving at: ");
        System.out.printf("%d ms",startTime);
        System.out.println();
        TaskData.ExecutionPlan executionPlan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node node = executionPlan.addNode(DataType.URL);
        String sendURL;
        Tracklet testTracklet = testTracklets[0];
        byte[][] serializedData = new byte[1000][];
        for (int i = 0 ; i < 1000 ; ++i) {

            sendURL = "hdfs://kman-nod1:8020/user/labadmin/yangzhou/" + i;

            Path URL = new Path(sendURL);
            //Checking the URL;
            if (! hdfs.exists(URL)){
                hdfs.mkdirs(URL);
            }
            Kafka_Url_Test.testTrackletsSaving(sendURL,
                                        testTracklet,
                                        hdfs);

            serializedData[i] =  serialize(new TaskData(node.createInputPort(TEST_URL_SAVE_RETRIVE_PORT),
                    executionPlan,sendURL));
            sendWithLog("topicForURLTest",UUID.randomUUID().toString(),
                    serializedData[i] ,producer,logger);
            System.out.println();
            System.out.printf("Serialized Task: %d bytes",serializedData[i].length);
            System.out.println();
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Finished saving 1000 tracklets at: %d ms",endTime);
        System.out.println();

        hdfs.close();
        tmpHDFS.close();


    }


}
