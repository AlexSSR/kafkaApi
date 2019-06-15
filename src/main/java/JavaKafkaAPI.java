import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class JavaKafkaAPI {


    private String topic ;

    private Producer<Integer,String> producer ;

    private JavaKafkaAPI(){

    }


    private static KafkaConsumer kafkaConsumer(String kafkaPath){
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaPath);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }

    private static KafkaProducer kafkaProducer(String kafkaPath){
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaPath);
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
        String kafkaPath = "10.11.6.3:9092,10.11.6.6:9092";
        String topic = "us_general";

         final KafkaConsumer kafkaConsumer = kafkaConsumer(kafkaPath);
         final KafkaProducer kafkaProducer = kafkaProducer(kafkaPath);

//        KafkaConsumer.subscribe(Arrays.asList(topic));
//        TopicPartition par1 = new TopicPartition("real_test", 0);
//        kafkaConsumer.assign(Arrays.asList(par1));

        kafkaConsumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

                Map map = kafkaConsumer.beginningOffsets(collection);

                for (Object o : map.entrySet()) {
                    kafkaConsumer.seekToBeginning(collection);
                }
        }});

        int i = 0 ;

        try {
            while (true) {

                i++;

                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records){

                    int randomInt = random.nextInt(100);
                    int nextInt = random.nextInt(100);
                    int acessPad = 0;
                    int stopPad = 0;
                    String msg = record.value();
                    String dataFormat = null;

                    //TODO : 一波逻辑

                    if (randomInt%2 == 0){
                        acessPad = nextInt;
                    }else {
                        stopPad =nextInt;
                    }

                    String[] array = msg.split(" ");

                    Map<String, String> map = Splitter.on(",").withKeyValueSeparator(":").split(array[4]);

                    LinkedHashMap<String, String> hashMap = new LinkedHashMap<String, String>();

                    for (Map.Entry<String, String> entry : map.entrySet()) {

                        String key = entry.getKey();
                        String value = entry.getValue();

                       if (key.equals("2208")){
                           hashMap.put(key,String.valueOf(acessPad));
                       }else if (key.equals("2209")){
                           hashMap.put(key,String.valueOf(stopPad));
                       }
                       else if (key.equals("9999")){

                           String specialDatetime = null;
                           if (value.endsWith("}")){
                                specialDatetime = value.substring(0, 14);
                           }else {
                               specialDatetime =value;
                           }

                           if (i <= 1000){
                               dataFormat = "yyyyMM15HHmmss";
                           }else if (i <= 2000){
                               dataFormat = "yyyyMM16HHmmss";
                           }else {
                               dataFormat = "yyyyMM14HHmmss";
                           }

                           String yyyyMMddHHmmss = LocalDateTime.parse(specialDatetime, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).plus(37, ChronoUnit.SECONDS).format(DateTimeFormatter.ofPattern(dataFormat));

                           String  concat = null;
                           if (yyyyMMddHHmmss.length() == 14){
                                concat = yyyyMMddHHmmss.concat("}");
                           }else {
                                concat = yyyyMMddHHmmss;
                           }

                           hashMap.put("9999",concat);
                       }
                       else if (key.equals("2205")){
                           hashMap.put(key,value);
                           hashMap.put("2208",String.valueOf(acessPad));
                           hashMap.put("2209",String.valueOf(stopPad));
                       }
                       else if (key.equals("2000")){


                           if (i <= 1000){
                               dataFormat = "yyyyMM15HHmmss";
                           }else if (i <= 2000){
                               dataFormat = "yyyyMM16HHmmss";
                           }else {
                               dataFormat = "yyyyMM14HHmmss";
                           }
                           String yyyyMMddHHmmss = LocalDateTime.parse(value, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).plus(37, ChronoUnit.SECONDS).format(DateTimeFormatter.ofPattern(dataFormat));
                           hashMap.put("2000",yyyyMMddHHmmss);
                       }
                       else {
                          hashMap.put(key,value);
                     }
                 }

                   String mapString = Joiner.on(",").withKeyValueSeparator(":").join(hashMap);

                   array[4] = mapString;

                   String joinResult = Joiner.on(" ").join(array);

                    System.out.println(joinResult);

                kafkaProducer.send(new ProducerRecord<String, String>
                        (topic,joinResult));

                    System.out.println(i);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
