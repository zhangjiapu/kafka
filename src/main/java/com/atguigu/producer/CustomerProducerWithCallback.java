package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @ClassName CustomerProducerWithCallback
 * @Description TODO
 * @Author 张家谱
 * @Date 2019/8/4 17:45
 * @Version 1.0
 **/

public class CustomerProducerWithCallback {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("myTopic", String.valueOf(i));
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 发送数据之后，拿到的数据在kafka的信息
                    System.out.println(metadata.offset() + metadata.partition()
                            + metadata.serializedKeySize() + metadata.serializedValueSize()
                            + metadata.timestamp());
                }
            });

        }



    }



}
