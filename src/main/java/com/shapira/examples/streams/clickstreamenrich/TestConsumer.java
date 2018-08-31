package com.shapira.examples.streams.clickstreamenrich;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * @desc: 模拟消费者
 * @author: panqiong
 * @date: 2018/8/25
 */
@Slf4j
public class TestConsumer {


    /**
     * 自动提交偏移量
     */
    public void autoCommit(){
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKER);
        props.put("group.id", "group5");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(Constants.USER_ACTIVITY_TOPIC));
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                //System.out.printf(new Date()+">> offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                //System.out.println(new Date()+">> record.toString() = " + record.toString());
                String key = record.key();
                String value = record.value();
                log.info("key:"+key+"   value:"+value);

            }
        }
    }

    public static void main(String[] args) {
        new TestConsumer().autoCommit();
    }


}
