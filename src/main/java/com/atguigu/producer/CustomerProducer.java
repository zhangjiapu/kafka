package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/*
 * @ClassName CustomerProducer
 * @Description TODO
 * @Author 张家谱
 * @Date 2019/8/3 15:27
 * @Version 1.0
 **/

public class CustomerProducer {

    public static void main(String[] args) {

        // 配置连接属性
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("mytopic", String.valueOf(i)));
        }

        //关闭资源
        kafkaProducer.close();
    }

}
