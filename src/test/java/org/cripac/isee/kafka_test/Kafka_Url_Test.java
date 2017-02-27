package org.cripac.isee.kafka_test;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.cripac.isee.pedestrian.tracking.BasicTracker;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;


/**
 * Created by yang on 17-2-22.
 */
public class Kafka_Url_Test {
   private Tracklet[] textTracker(String videoURL) throws Exception {
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
       retrun testTracklets;
   }

}
