package org.cripac.isee.kafka_test;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import org.cripac.isee.pedestrian.tracking.BasicTracker;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.data.DataManagingApp;
import org.cripac.isee.vpe.util.SerializationHelper;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.spark_project.guava.collect.ContiguousSet;
import org.spark_project.guava.collect.DiscreteDomain;
import org.spark_project.guava.collect.Range;
import scala.Tuple2;

import javax.annotation.Nonnull;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;


/**
 * Created by yang on 17-2-22.
 */
public class Kafka_Url_Test {
    public static HadoopHelper HadoopTool = new HadoopHelper();
    public static Tracklet[] testTracker(String videoURL) throws Exception {
            System.out.println("Performing validness test...");

            System.out.println("Reading video...");
            InputStream videoBytes = new FileInputStream(videoURL);

            System.out.println("Creating tracker...");
            BasicTracker Pretrack = new BasicTracker(
                    IOUtils.toByteArray(new FileInputStream(
                            "conf/"
                                    + PedestrianTrackingApp.APP_NAME
                                    + "/isee-basic/CAM01_0.conf")),
                    new ConsoleLogger(Level.DEBUG));

            System.out.println("Start tracking...");
            Tracklet[] testTracklets = Pretrack.track(videoBytes);

            System.out.println("Tracked " + testTracklets.length + " pedestrians!");
            for (Tracklet tracklet : testTracklets) {
                System.out.println(tracklet);
            }
            return testTracklets;
    }

    public static void testTrackletsSaving(@Nonnull String storeDir,
                                           @Nonnull Tracklet tracklet,
                                           @Nonnull FileSystem hdfs) throws Exception{

        //System.out.println("Tracklet " + tracklet.id + " is saving into the HDFS: " + storeDir);

        HadoopTool.storeTracklet(storeDir,tracklet,hdfs);

        //System.out.println("Tracklet " + tracklet.id + " is saved!");
    }
    public static Tracklet testTrackletsRetrieving(@Nonnull String storeDir) throws URISyntaxException, IOException{

        //System.out.println("Tracklet is retrieving from the HDFS: " + storeDir);

        Tracklet testTracklet = HadoopTool.retrieveTracklet(storeDir);

        //System.out.println("Tracklet " + testTracklet.id + " is retrieved!");
        return testTracklet;
    }

}


