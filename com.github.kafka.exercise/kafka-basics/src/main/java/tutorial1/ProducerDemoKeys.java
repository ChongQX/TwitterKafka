package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        //https://kafka.apache.org/documentation/#producerconfigs to check for required fields
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<10; i++) {

            String topic = "1st_topic";
            String value = "hello" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            //create producer record
//            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            //providing a key will guarantee same key always goes to same partition
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key: {}", key);

            //send data (asynchronous)
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is sent successfully or exception thrown
                    if (null == e) {
                        //record sent successfully
                        logger.info("Received new metadata : " +
                                        "Topic: {} " +
                                        "Partition: {} " +
                                        "Offset: {} " +
                                        "Timestamp: {}",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }

                }
            }).get(); //block .send() to make it synchronous - NEVER do this is production
        }

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }

}
