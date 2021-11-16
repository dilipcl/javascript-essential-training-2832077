package com.github.dcl;

import java.beans.PropertyEditor;
import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.apache.kafka.common.serialization.StringSerializer;

public class SampleProducer {
    public static void main(String[] args) {
        // System.out.println("Hello Chakkare!!!");
        
        String bootStrapServer="127.0.0.1:9092";

        Properties properties= new Properties();

        // Producer Properties
        // properties.setProperty("bootstrap.servers", bootStrapServer);
        // properties.setProperty("key.serializer", StringSerializer.class.getName());
        // properties.setProperty("value.serializer",StringSerializer.class.getName())

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer 

        KafkaProducer<String,String> producer= new KafkaProducer<String,String>(properties);

        //create a topic
        ProducerRecord<String,String> record = new  ProducerRecord<String,String>("first_topic", "Hello from java.. buhaha6");

        //send the data
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
