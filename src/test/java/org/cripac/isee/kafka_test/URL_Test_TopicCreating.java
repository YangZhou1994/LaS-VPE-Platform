package org.cripac.isee.kafka_test;

import kafka.utils.ZkUtils;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;

/**
 * Created by yang on 17-2-27.
 */
public class URL_Test_TopicCreating {
    public static void main(String[] arg){
        System.out.println("Connecting to zookeeper: " + "ktask-nod1:2181,ktask-nod2:2181,ktask-nod3:2181,ktask-nod4:2181");
        final ZkUtils zkUtils = KafkaHelper.createZKUtils("ktask-nod1:2181,ktask-nod2:2181,ktask-nod3:2181,ktask-nod4:2181",
                30000,
                300000);
        KafkaHelper.createTopicIfNotExists(zkUtils,
                    "topicForURLTest",
                    10,
                    2);
        System.out.println("Topics checked!");
    }
}
