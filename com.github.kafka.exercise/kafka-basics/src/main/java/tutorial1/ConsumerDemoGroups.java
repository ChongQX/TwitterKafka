package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {

    public static void main(String[] args) {
/*
        kafka-consumer-groups --bootstrap-server localhost:9092 --group my-4th-app --describe
        if CURRENT-OFFSET == LOG-END-OFFSET or LAG == 0 then it won't read any msgs
        solution: reset or change groupId
*/

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-5th-app";
        String topic = "1st_topic";

        //https://kafka.apache.org/documentation/#consumerconfigs check required fields

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
/*
        AUTO_OFFSET_RESET_CONFIG:
            earliest - from beginning
            latest - only from new msg onwards
            none - throw error
*/

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to list of topic(s)
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data   *consumer read by partition eg. 1 then 0 then 2
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: {}, Value: {}" , record.key(), record.value());
                logger.info("Partition: {}, Offset: {}" , record.partition(), record.offset());
            }
        }

    }
}
