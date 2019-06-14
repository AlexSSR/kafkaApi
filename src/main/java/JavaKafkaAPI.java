import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class JavaKafkaAPI {


    private String topic ;

    private Producer<Integer,String> producer ;


    private static KafkaConsumer kafkaConsumer(String localhost){
        Properties props = new Properties();
        props.put("bootstrap.servers", localhost);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);

    }

    private static KafkaProducer kafkaProducer(String localhost){
        Properties props = new Properties();
        props.put("bootstrap.servers", localhost);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String,String>(props);
    }

    public static void main(String[] args) {

        Random random = new Random();

        final KafkaConsumer kafkaConsumer = kafkaConsumer("localhost:9093");
        final KafkaProducer kafkaProducer = kafkaProducer("localhost:9093");

//        KafkaConsumer.subscribe(Arrays.asList("real_test"));
//        TopicPartition par1 = new TopicPartition("real_test", 0);
//        kafkaConsumer.assign(Arrays.asList(par1));

        kafkaConsumer.subscribe(Collections.singletonList("real_test"), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

                Map map = kafkaConsumer.beginningOffsets(collection);

                for (Object o : map.entrySet()) {
                    kafkaConsumer.seekToBeginning(collection);
                }
        }});

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records){

                    int randomInt = random.nextInt(100);

                    String value = record.value();

                    //TODO : 一波逻辑

//                kafkaProducer.send(new ProducerRecord<String, String>
//                        ("real_test","test_data",String.valueOf(Double.parseDouble(value)+1)));



                    System.out.println(value);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
