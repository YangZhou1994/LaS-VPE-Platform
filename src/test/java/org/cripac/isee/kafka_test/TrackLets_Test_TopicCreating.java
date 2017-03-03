package org.cripac.isee.kafka_test;

import kafka.utils.ZkUtils;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;

/**
 * Created by yang on 17-3-1.
 */
public class TrackLets_Test_TopicCreating {
    public static void main(String[] args){
        System.out.println("Connecting to zookeeper: " + "ktask-nod1:2181,ktask-nod2:2181,ktask-nod3:2181,ktask-nod4:2181");
        final ZkUtils zkUtils = KafkaHelper.createZKUtils("ktask-nod1:2181,ktask-nod2:2181,ktask-nod3:2181,ktask-nod4:2181",
                30000,
                300000);
        KafkaHelper.createTopicIfNotExists(zkUtils,
                "topicForTrackletsSRTest",
                10,
                2);
        System.out.println("Topics checked!");
    }
}
