package org.cripac.isee.kafka_test;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
   public static class testTrack {
        private Tracklet[] testTracker(String videoURL) throws Exception {
            System.out.println("Performing validness test...");

            System.out.println("Reading video...");
            InputStream videoBytes = new FileInputStream("src/test/resources/20131220184349-20131220184937.h264");

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
    }

    public static class TrackletSavingStream extends Stream {
        public static final String NAME = "tracklet-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_TRACKLET_SAVING_PORT =
                new Port("pedestrian-tracklet-saving", DataType.TRACKLET);
        private static final long serialVersionUID = 2820895755662980265L;
        private final Singleton<FileSystem> hdfsSingleton;
        private final String metadataDir;
        private final Singleton<KafkaProducer<String, byte[]>> packingJobProducerSingleton;

        TrackletSavingStream(@Nonnull DataManagingApp.AppPropertyCenter propCenter) throws Exception {
            super("SavingTracklet", propCenter);

            hdfsSingleton = new Singleton<>(new HDFSFactory());
            metadataDir = propCenter.metadataDir;
            packingJobProducerSingleton = new Singleton<>(
                    new KafkaProducerFactory<>(propCenter.getKafkaProducerProp(false))
            );
        }

        /**
         * Store the track to the HDFS.
         *
         * @param storeDir The directory storing the track.
         * @param tracklet The track to store.
         * @throws IOException On failure creating and writing files in HDFS.
         */
        private void storeTracklet(@Nonnull String storeDir,
                                   @Nonnull Tracklet tracklet) throws Exception {
            final FileSystem hdfs = hdfsSingleton.getInst();

            // Write verbal informations with Json.
            final FSDataOutputStream outputStream = hdfs.create(new Path(storeDir + "/info.txt"));

            // Customize the serialization of bounding box in order to ignore patch data.
            final GsonBuilder gsonBuilder = new GsonBuilder();
            final JsonSerializer<Tracklet.BoundingBox> bboxSerializer = (box, typeOfBox, context) -> {
                JsonObject result = new JsonObject();
                result.add("x", new JsonPrimitive(box.x));
                result.add("y", new JsonPrimitive(box.y));
                result.add("width", new JsonPrimitive(box.width));
                result.add("height", new JsonPrimitive(box.height));
                return result;
            };
            gsonBuilder.registerTypeAdapter(Tracklet.BoundingBox.class, bboxSerializer);

            // Write serialized basic information of the tracklet to HDFS.
            outputStream.writeBytes(gsonBuilder.create().toJson(tracklet));
            outputStream.close();

            // Write frames concurrently.
            ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                    .parallelStream()
                    .forEach(idx -> {
                        final Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];

                        // Use JavaCV to encode the image patch
                        // into JPEG, stored in the memory.
                        final BytePointer inputPointer = new BytePointer(bbox.patchData);
                        final opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
                        final BytePointer outputPointer = new BytePointer();
                        imencode(".jpg", image, outputPointer);
                        final byte[] bytes = new byte[(int) outputPointer.limit()];
                        outputPointer.get(bytes);

                        // Output the image patch to HDFS.
                        final FSDataOutputStream imgOutputStream;
                        try {
                            imgOutputStream = hdfs.create(new Path(storeDir + "/" + idx + ".jpg"));
                            imgOutputStream.write(bytes);
                            imgOutputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        // Free resources.
                        image.release();
                        inputPointer.deallocate();
                        outputPointer.deallocate();
                    });
        }

        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<String, TaskData>> globalStreamMap) {// Save tracklets.
            this.filter(globalStreamMap, PED_TRACKLET_SAVING_PORT)
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                        final Logger logger = loggerSingleton.getInst();
                        try {
                            // RuntimeException: No native JavaCPP library
                            // in memory. (Has Loader.load() been called?)
                            Loader.load(org.bytedeco.javacpp.helper.opencv_core.class);
                            Loader.load(opencv_imgproc.class);

                            final FileSystem hdfs = hdfsSingleton.getInst();

                            final String taskID = kv._1();
                            final TaskData taskData = kv._2();
                            final Tracklet tracklet = (Tracklet) taskData.predecessorRes;
                            final int numTracklets = tracklet.numTracklets;

                            final String videoRoot = metadataDir + "/" + tracklet.id.videoID;
                            final String taskRoot = videoRoot + "/" + taskID;
                            final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                            final Path storePath = new Path(storeDir);
                            if (hdfs.exists(storePath)
                                    || hdfs.exists(new Path(videoRoot + "/" + taskID + ".har"))) {
                                logger.warn("Duplicated storing request for " + tracklet.id);
                            } else {
                                hdfs.mkdirs(new Path(storeDir));
                                storeTracklet(storeDir, tracklet);

                                // Check packing.
                                /*KafkaHelper.sendWithLog(DataManagingApp.TrackletPackingThread.JOB_TOPIC,
                                        taskID,
                                        SerializationHelper.serialize(new Tuple2<>(tracklet.id.videoID, numTracklets)),
                                        packingJobProducerSingleton.getInst(),
                                        logger);*/
                            }
                        } catch (Exception e) {
                            logger.error("During storing tracklets.", e);
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_TRACKLET_SAVING_PORT);
        }
    }
}


