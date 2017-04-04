package edu.uprm.edu.bigdata.kafkatohdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by omar on 4/4/17.
 */
public class KafkaToHDFS {
    public static final String hdfs_main = "hdfs://master:9000";
    public static final String out_hdfs_file = "/home/omar/trump_tweets_stream";

    public static final String bootstrapServers = "data04:9092";
    public static final String groupID = "trump-consumer-group";
    private static final List<String> topics = Arrays.asList("trump");

    private static Consumer<Long, String> consumer;
    private final Properties properties = new Properties();

    public KafkaToHDFS() {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put("key.deserializer", LongDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", groupID);

        consumer = new KafkaConsumer<Long, String>(properties);
    }

    public static void main(String[] args) throws Exception {
        if (consumer == null) new KafkaToHDFS();

        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy-hms");
        String out_file = out_hdfs_file + "_" + sdf.format(new Date()) + ".txt";

        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI(hdfs_main), configuration);
        Path file = new Path(hdfs_main + out_file);
        FSDataOutputStream fs;

        if (!hdfs.exists(file)) {
            fs = hdfs.create(file);
        } else {
            System.out.println("File " + file.toString() + "already exists!");
            return;
        }

        try {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(1000);

                for (ConsumerRecord<Long, String> record : records) {
                    fs.write((record.value() + "\n").getBytes("UTF-8"));
                    fs.hflush();
                }
            }
        }  catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fs != null) fs.close();
            consumer.close();
        }
    }
}
