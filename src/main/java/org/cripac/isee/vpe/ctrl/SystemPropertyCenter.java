/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cripac.isee.vpe.ctrl;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.commons.cli.*;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.spark.launcher.SparkLauncher;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * The SystemPropertyCenter class is responsible of managing the properties of
 * the systems. There are some properties predefined, and they can be
 * overwritten by command options or an extern property file. It can also
 * generate back command options for uses like SparkSubmit.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SystemPropertyCenter implements Serializable {

    private static final long serialVersionUID = -6642856932636724919L;
    // Logger for parsing.
    private transient Logger logger = new ConsoleLogger(Level.INFO);

    // Zookeeper properties
    public String zkConn = "localhost:2181";
    public int sessionTimeoutMs = 10 * 10000;
    // Kafka properties
    public String kafkaBootstrapServers = "localhost:9092";
    public int kafkaNumPartitions = 1;
    public int kafkaReplFactor = 1;
    private int kafkaMsgMaxBytes = 100000000;
    private int kafkaSendMaxSize = 100000000;
    private int kafkaRequestTimeoutMs = 60000;
    private int kafkaFetchTimeoutMs = 60000;
    public String kafkaLocationStrategy = "PreferBrokers";
    // Spark properties
    public String checkpointRootDir = "checkpoint";
    public String metadataDir = "metadata";
    public String sparkMaster = "local[*]";
    public String sparkDeployMode = "client";
    String[] appsToStart = null;
    // Caffe properties
    public int caffeGPU = -1;
    /**
     * Memory per executor (e.g. 1000M, 2G) (Default: 1G)
     */
    private String executorMem = "1G";
    /**
     * Number of executors to run (Default: 2)
     */
    private int numExecutors = 2;
    /**
     * Number of cores per executor (Default: 1)
     */
    private int executorCores = 1;
    /**
     * Total cores for all executors (Spark standalone and Mesos only).
     */
    private int totalExecutorCores = 1;
    /**
     * Memory for driver (e.g. 1000M, 2G) (Default: 1024 Mb)
     */
    private String driverMem = "1G";
    /**
     * Number of cores used by the driver (Default: 1)
     */
    private int driverCores = 1;
    /**
     * A YARN node label expression that restricts the set of nodes AM will be scheduled on.
     * Only versions of YARN greater than or equal to 2.6 support node label expressions,
     * so when running against earlier versions, this property will be ignored.
     * <p>
     * To enable label-based scheduling,
     * see https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeLabel.html
     */
    private String yarnAmNodeLabelExpression = "";
    /**
     * The hadoop queue to use for allocation requests (Default: 'default')
     */
    private String hadoopQueue = "default";
    private String sysPropFilePath = "conf/system.properties";
    /**
     * Application-specific property file. Properties loaded from it
     * will override those loaded from the system property file.
     * Leaving it as null will let the system automatically find
     * that in default places according to the application specified.
     */
    private String appPropFilePath = null;
    private String sparkConfFilePath = ConfManager.CONF_DIR + "/spark-defaults.conf";
    private String log4jPropFilePath = ConfManager.CONF_DIR + "/log4j.properties";
    private String hdfsDefaultName = "localhost:9000";
    private String jarPath = "bin/vpe-platform.jar";
    /**
     * Duration for buffering results.
     */
    public int bufDuration = 600000;
    /**
     * Duration of spark batches.
     */
    public int batchDuration = 2000;
    /**
     * Whether to print verbose running information.
     */
    public boolean verbose = false;

    /**
     * Subclasses can continue to analyze this property storage.
     */
    protected Properties sysProps = new Properties();

    /**
     * Construction function supporting allocating a SystemPropertyCenter then
     * filling in the properties manually.
     */
    public SystemPropertyCenter() throws SAXException, ParserConfigurationException, URISyntaxException {
        this(new String[0]);
    }

    public SystemPropertyCenter(@Nonnull String[] args)
            throws URISyntaxException, ParserConfigurationException, SAXException {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message.");
        options.addOption("v", "verbose", false, "Display debug information.");
        options.addOption("a", "application", true, "Application specified to run.");
        options.addOption(null, "spark-property-file", true, "Path of the spark property file.");
        options.addOption(null, "system-property-file", true, "Path of the system property file.");
        options.addOption(null, "app-property-file", true, "Path of the application-specific system property file.");
        options.addOption(null, "log4j-property-file", true, "Path of the log4j property file.");
        options.addOption(null, "report-listening-addr", true, "Address of runtime report listener.");
        options.addOption(null, "report-listening-topic", true, "Port of runtime report listener.");
        CommandLine commandLine;

        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            logger.debug("Try using '-h' for more information.");
            System.exit(0);
            return;
        }

        if (commandLine.hasOption('h')) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("LaS-VPE Platform", options);
            System.exit(0);
            return;
        }

        verbose = commandLine.hasOption('v');
        if (verbose) {
            logger.setLevel(Level.DEBUG);
        }

        if (commandLine.hasOption('a')) {
            appsToStart = commandLine.getOptionValues('a');
            logger.debug("To run application:");
            for (String app : appsToStart) {
                logger.debug("\t\t" + app);
            }
        }

        if (commandLine.hasOption("system-property-file")) {
            sysPropFilePath = commandLine.getOptionValue("system-property-file");
        }
        if (commandLine.hasOption("log4j-property-file")) {
            log4jPropFilePath = commandLine.getOptionValue("log4j-property-file");
        }
        if (commandLine.hasOption("spark-property-file")) {
            sparkConfFilePath = commandLine.getOptionValue("spark-property-file");
        }
        if (commandLine.hasOption("app-property-file")) {
            appPropFilePath = commandLine.getOptionValue("app-property-file");
        }

        // Load properties from file.
        BufferedInputStream propInputStream;
        try {
            if (sysPropFilePath.contains("hdfs:/")) {
                // TODO: Check if can load property file from HDFS.
                logger.debug("Loading system-wise default properties using HDFS platform from "
                        + sysPropFilePath + "...");
                final FileSystem hdfs = FileSystem.get(new URI(sysPropFilePath), HadoopHelper.getDefaultConf());
                final FSDataInputStream hdfsInputStream = hdfs.open(new Path(sysPropFilePath));
                propInputStream = new BufferedInputStream(hdfsInputStream);
            } else {
                final File propFile = new File(sysPropFilePath);
                logger.debug("Loading system-wise default properties locally from "
                        + propFile.getAbsolutePath() + "...");
                propInputStream = new BufferedInputStream(new FileInputStream(propFile));
            }
            sysProps.load(propInputStream);
            propInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Couldn't find system-wise default property file at specified path: \""
                    + sysPropFilePath + "\"!\n");
            logger.error("Try use '-h' for more information.");
            System.exit(0);
            return;
        }

        if (appPropFilePath != null) {
            try {
                if (appPropFilePath.contains("hdfs:/")) {
                    // TODO: Check if can load property file from HDFS.
                    logger.debug("Loading application-specific properties"
                            + " using HDFS platform from "
                            + appPropFilePath + "...");
                    final FileSystem hdfs = FileSystem.get(new URI(appPropFilePath), HadoopHelper.getDefaultConf());
                    final FSDataInputStream hdfsInputStream = hdfs.open(new Path(appPropFilePath));
                    propInputStream = new BufferedInputStream(hdfsInputStream);
                } else {
                    final File propFile = new File(appPropFilePath);
                    logger.debug("Loading application-specific properties locally from "
                            + propFile.getAbsolutePath() + "...");
                    propInputStream = new BufferedInputStream(new FileInputStream(propFile));
                }
                sysProps.load(propInputStream);
                propInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("Couldn't find application-specific property file at specified path: \""
                        + appPropFilePath + "\"!\n");
                logger.error("Try use '-h' for more information.");
                System.exit(0);
                return;
            }
        }

        // Digest the settings.
        for (Entry<Object, Object> entry : sysProps.entrySet()) {
            if (verbose) {
                logger.debug("Read from property file: " + entry.getKey()
                        + "=" + entry.getValue());
            }
            switch ((String) entry.getKey()) {
                case "zookeeper.connect":
                    zkConn = (String) entry.getValue();
                    break;
                case "kafka.bootstrap.servers":
                    kafkaBootstrapServers = (String) entry.getValue();
                    break;
                case "kafka.partitions":
                    kafkaNumPartitions = new Integer((String) entry.getValue());
                    break;
                case "kafka.replication.factor":
                    kafkaReplFactor = new Integer((String) entry.getValue());
                    break;
                case "kafka.fetch.max.size":
                    kafkaMsgMaxBytes = new Integer((String) entry.getValue());
                    break;
                case "kafka.location.strategy":
                    kafkaLocationStrategy = (String) entry.getValue();
                    break;
                case "spark.checkpoint.dir":
                    checkpointRootDir = (String) entry.getValue();
                    break;
                case "vpe.metadata.dir":
                    metadataDir = (String) entry.getValue();
                    break;
                case "spark.master":
                    sparkMaster = (String) entry.getValue();
                    break;
                case "spark.deploy.mode":
                    sparkDeployMode = (String) entry.getValue();
                    break;
                case "vpe.platform.jar":
                    jarPath = (String) entry.getValue();
                    break;
                case "spark.yarn.am.nodeLabelExpression":
                    yarnAmNodeLabelExpression = (String) entry.getValue();
                    break;
                case "hdfs.default.name":
                    hdfsDefaultName = (String) entry.getValue();
                    break;
                case "executor.num":
                    numExecutors = new Integer((String) entry.getValue());
                    break;
                case "executor.memory":
                    executorMem = (String) entry.getValue();
                    break;
                case "executor.cores":
                    executorCores = new Integer((String) entry.getValue());
                    break;
                case "total.executor.cores":
                    totalExecutorCores = new Integer((String) entry.getValue());
                    break;
                case "driver.memory":
                    driverMem = (String) entry.getValue();
                    break;
                case "driver.cores":
                    driverCores = new Integer((String) entry.getValue());
                    break;
                case "hadoop.queue":
                    hadoopQueue = (String) entry.getValue();
                    break;
                case "vpe.recv.parallel":
                    break;
                case "vpe.buf.duration":
                    bufDuration = new Integer((String) entry.getValue());
                    break;
                case "vpe.batch.duration":
                    batchDuration = new Integer((String) entry.getValue());
                    break;
                case "kafka.send.max.size":
                    kafkaSendMaxSize = new Integer((String) entry.getValue());
                    break;
                case "kafka.request.timeout.ms":
                    kafkaRequestTimeoutMs = new Integer((String) entry.getValue());
                    break;
                case "kafka.fetch.timeout.ms":
                    kafkaFetchTimeoutMs = new Integer((String) entry.getValue());
                    break;
                case "caffe.gpu":
                    caffeGPU = new Integer((String) entry.getValue());
                    break;
            }
        }
    }

    /**
     * Generate command line options for SparkSubmit client, according to the
     * stored properties.
     *
     * @return An array of string with format required by SparkSubmit client.
     */
    private String[] getArgs() {
        ArrayList<String> optList = new ArrayList<>();

        if (verbose) {
            optList.add("-v");
        }

        if (appPropFilePath != null && new File(appPropFilePath).exists()) {
            optList.add("--app-property-file");
            optList.add(new File(appPropFilePath).getName());
        }

        optList.add("--system-property-file");
        optList.add(new File(sysPropFilePath).getName());

        optList.add("--log4j-property-file");
        if (sparkMaster.toLowerCase().contains("yarn")) {
            optList.add("log4j.properties");
        } else if (sparkMaster.toLowerCase().contains("local")) {
            optList.add(log4jPropFilePath);
        } else {
            throw new NotImplementedException(
                    "System is currently not supporting deploy mode: " + sparkMaster);
        }

        return Arrays.copyOf(optList.toArray(), optList.size(), String[].class);
    }

    SparkLauncher GetSparkLauncher(String appName) throws IOException, NoAppSpecifiedException {
        SparkLauncher launcher = new SparkLauncher()
                .setAppResource(jarPath)
                .setMainClass(AppManager.getMainClassName(appName))
                .setMaster(sparkMaster)
                .setAppName(appName)
                .setVerbose(verbose)
                .addFile(ConfManager.getConcatCfgFilePathList(","))
                .setConf(SparkLauncher.DRIVER_MEMORY, driverMem)
                .setConf(SparkLauncher.EXECUTOR_MEMORY, executorMem)
                .setConf(SparkLauncher.CHILD_PROCESS_LOGGER_NAME, appName)
                .setConf(SparkLauncher.EXECUTOR_CORES, "" + executorCores)
                .setConf("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
                .setConf("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
                .setConf("spark.yarn.am.nodeLabelExpression", yarnAmNodeLabelExpression)
                .addSparkArg("--driver-cores", "" + driverCores)
                .addSparkArg("--num-executors", "" + numExecutors)
                .addSparkArg("--total-executor-cores", "" + totalExecutorCores)
                .addSparkArg("--queue", hadoopQueue)
                .addAppArgs(getArgs());
        if (sparkConfFilePath != null) {
            if (new File(sparkConfFilePath).exists()) {
                launcher = launcher.setPropertiesFile(sparkConfFilePath);
            } else {
                logger.warn("Spark configuration file " + sparkConfFilePath + " does not exist!");
            }
        }
        if (log4jPropFilePath != null) {
            if (new File(log4jPropFilePath).exists()) {
                launcher = launcher.addFile(log4jPropFilePath);
            } else {
                logger.warn("Loj4j configuration file " + log4jPropFilePath + " does not exist!");
            }
        }
        if (sysPropFilePath != null) {
            if (new File(sysPropFilePath).exists()) {
                launcher = launcher.addFile(sysPropFilePath);
            } else {
                logger.warn("System configuration file " + sysPropFilePath + " does not exist!");
            }
            launcher = launcher.addFile(sysPropFilePath);
        }
        if (appPropFilePath != null) {
            if (new File(appPropFilePath).exists()) {
                launcher = launcher.addFile(appPropFilePath);
            } else {
                logger.warn("App configuration file " + appPropFilePath + " does not exist!");
            }
        }
        return launcher;
    }

    /**
     * Thrown when no application is specified in any possible property sources.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class NoAppSpecifiedException extends Exception {
        private static final long serialVersionUID = -8356206863229009557L;

        public NoAppSpecifiedException(String message) {
            super(message);
        }
    }

    public Properties getKafkaProducerProp(boolean isStringValue) {
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaSendMaxSize);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                isStringValue ? StringSerializer.class : ByteArraySerializer.class);
        producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaSendMaxSize);
        producerProp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaRequestTimeoutMs);
        producerProp.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return producerProp;
    }

    public Properties getKafkaConsumerProp(String group, boolean isStringValue) {
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                isStringValue ? StringDeserializer.class : ByteArrayDeserializer.class);
        consumerProp.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaFetchTimeoutMs);
        return consumerProp;
    }

    public Map<String, Object> getKafkaParams(String group) {
        Map<String, Object> kafkaParams = new Object2ObjectOpenHashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, kafkaMsgMaxBytes);
        kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, kafkaMsgMaxBytes);
        kafkaParams.put("fetch.message.max.bytes", kafkaMsgMaxBytes);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, kafkaMsgMaxBytes);
        kafkaParams.put(ConsumerConfig.SEND_BUFFER_CONFIG, kafkaMsgMaxBytes);
        kafkaParams.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaFetchTimeoutMs);
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return kafkaParams;
    }
}
