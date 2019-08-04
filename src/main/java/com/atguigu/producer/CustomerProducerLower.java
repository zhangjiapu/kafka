package com.atguigu.producer;

/**
 * @ClassName CustomerProducerLower
 * @Description TODO
 * @Author 张家谱
 * @Date 2019/8/4 12:41
 * @Version 1.0
 **/

import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据指定的Topic Partition Offset来获取数据
 * 1. 跟集群连接上
 * 2. 找到topic
 * 3. 找到分区
 * 4. 获取分区的leader
 * 5. 获取分区和副本信息
 *
 */
public class CustomerProducerLower {

    public static void main(String[] args) {

        //定义相关参数
        //kafka集群
        ArrayList<String> brokers = new ArrayList<String>();
        brokers.add("hadoop102");
        brokers.add("hadoop103");
        brokers.add("hadoop104");

        //端口号
        int port = 9092;

        //主题
        String topic = "second";

        //分区
        int partition = 0;

        //offset
        long offset = 2l;

        // 找到leader
        CustomerProducerLower customerProducerLower = new CustomerProducerLower();
        BrokerEndPoint leader = customerProducerLower.findLeader(brokers,port,topic,partition);

        // 从leader获取数据
        customerProducerLower.getData(leader,offset,brokers,port,topic,partition);

    }

    /**
     * 找分区的leader
     * @param brokers
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition){

        for (String broker : brokers) {
            // 创建获取分区leader的消费者对象
            SimpleConsumer myClient = new SimpleConsumer(broker, port, 1000, 1024 * 5, "myClient");

            //获取topic数据
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            TopicMetadataResponse topicMetadataResponse = myClient.send(topicMetadataRequest);
            List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();


            //从topic中获取partition数据
            for (TopicMetadata topicMetadata : topicMetadatas) {
                List<PartitionMetadata> partitionMetadataList = topicMetadata.partitionsMetadata();
                for (PartitionMetadata partitionMetadata : partitionMetadataList) {
                    // 分区选举Leader用
                    // partitionMetadata.isr();

                    // 分区副本
                    // partitionMetadata.replicas();

                    // 分区Leader
                    return partitionMetadata.leader();
                }

            }



        }


        return null;
    }


    /**
     * 获取数据
     * @param leader
     * @param offset
     */
    private void getData(BrokerEndPoint leader, long offset,List<String> brokers, int port, String topic, int partition){

        if(leader == null){
            System.out.println("Cannot Find The Leader!");
            return;
        }

        //不论是获取leader还是获取数据，都是通过这个客户端
        SimpleConsumer simpleConsumer = new SimpleConsumer(leader.host(), leader.port(), 1000, 1024 * 5, "getData");

        // 请求数据
        kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1024 * 4).build();
        FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);

        //解析返回的数据
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
            //消息的集合，逐一解码
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {

            //获取当前消息的偏移量
            long curr_offset = messageAndOffset.offset();

            //将消息解码
            ByteBuffer byteBuffer = messageAndOffset.message().payload();
            byte[] bytes = new byte[byteBuffer.limit()];
            byteBuffer.get(bytes);
            String messageContent = new String(bytes);

            System.out.println("Received Message: "+ messageContent);
        }


    }

}
