import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

/**
 * Created by omar on 4/3/17.
 */
public class TwitterStream {
    private static String topic = "trump";
    private static Producer<Long, String> producer;
    private final Properties properties = new Properties();

    public TwitterStream() {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "data04:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<Long, String>(properties);
    }

    public static void main(String[] args) throws Exception {
        if (producer == null) new TwitterStream();

        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("***")
                .setOAuthConsumerSecret("***")
                .setOAuthAccessToken("***")
                .setOAuthAccessTokenSecret("***")
                .setJSONStoreEnabled(true);

        twitter4j.TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        twitterStream.addListener(new StatusListener() {
            public void onStatus(Status status) {
                //System.out.println(status.getText());

                String statusJson = TwitterObjectFactory.getRawJSON(status);
                ProducerRecord<Long, String> rec = new ProducerRecord<Long, String>(topic, new Long(status.getId()), statusJson);

                if (producer == null) System.err.println("Producer is null!");
                else producer.send(rec);
            }

            public void onException(Exception ex) {
                System.err.println(ex.getMessage());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

            }

            public void onScrubGeo(long userId, long upToStatusId) {

            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onStallWarning(StallWarning warning) {

            }
        });

        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.track(new String[]{"Trump"});
        tweetFilterQuery.language(new String[]{"en"});

        twitterStream.filter(tweetFilterQuery);
    }
}
