package com.adc.da.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Vehicle2Kafka {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.11.35:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] str = new String[]{"{\"vin\":\"LGJE13EA8HM612678\",\"vehicleType\": \"EU001\",\"unit\": \"私人\" ,\"licensePlate\":\"津AF59415\",\"batteryType\":\"三元电池\",\"company\":\"ZQY11101\"}",
                "{\"vin\":\"LGJE13EA8HM612679\",\"vehicleType\": \"EU001\",\"unit\": \"私人\" ,\"licensePlate\":\"津AF59416\",\"batteryType\":\"三元电池\",\"company\":\"ZQY11101\"}",
                "{\"vin\":\"LGJE13EA8HM612680\",\"vehicleType\": \"EU001\",\"unit\": \"私人\" ,\"licensePlate\":\"津AF59417\",\"batteryType\":\"三元电池\",\"company\":\"ZQY11101\"}",
                "{\"vin\":\"LGJE13EA8HM612681\",\"vehicleType\": \"EU001\",\"unit\": \"私人\" ,\"licensePlate\":\"津AF59418\",\"batteryType\":\"三元电池\",\"company\":\"ZQY11102\"}",
                "{\"vin\":\"LGJE13EA8HM612682\",\"vehicleType\": \"EU001\",\"unit\": \"私人\" ,\"licensePlate\":\"津AF59419\",\"batteryType\":\"三元电池\",\"company\":\"ZQY11102\"}",
        };

        for (String s : str) {

            producer.send(new ProducerRecord<String, String>("vehicle", "0001",s));
            //System.out.println(s);
        }


        producer.close();

    }
}
