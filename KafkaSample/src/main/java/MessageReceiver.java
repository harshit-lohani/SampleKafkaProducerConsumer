import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MessageReceiver {

    Properties properties = new Properties();
    static final String TOPIC = "sample-topic";
    static final String GROUP = "sample-topic-group";


    private void init(){
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", GROUP);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);){
            consumer.subscribe(Arrays.asList(TOPIC));

            for(int i = 0 ; i < 1000 ; i++) {
                ConsumerRecords<String, String> records = consumer.poll(1000L);
                System.out.println("Size: " + records.count());
                for(ConsumerRecord<String, String> record : records){
                    System.out.println("Received a message: " + record.value());
                }
            }
            System.out.println("END");
        }
    }

    public static void main(String[] args) {
        MessageReceiver receiver = new MessageReceiver();
        receiver.init();
    }
}
