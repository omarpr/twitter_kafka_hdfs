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

import java.io.PrintWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by omar on 4/4/17.
 */
public class KafkaToHDFS {
    public static final String hdfs_main = "hdfs://master:9000";
    public static final String out_hdfs_file = "/home/omar/trump_tweets_stream.txt";

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

        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI(hdfs_main), configuration);
        Path file = new Path(hdfs_main + out_hdfs_file);

        if (!hdfs.exists(file)) {
            FSDataOutputStream fs = hdfs.create(file);
            fs.close();
        }

        try {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(1000);

                for (ConsumerRecord<Long, String> record : records) {
                    FSDataOutputStream fs = hdfs.append(file);
                    PrintWriter out = new PrintWriter(fs);

                    out.append(record.value() + "\n");

                    out.close();
                    fs.close();
                }
            }
        }  catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
