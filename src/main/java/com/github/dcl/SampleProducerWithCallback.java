package com.github.dcl;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleProducerWithCallback {
        public static void main(String[] args) throws InterruptedException, ExecutionException {
        // System.out.println("Hello Chakkare!!!");
        final Logger logger= LoggerFactory.getLogger(SampleProducerWithCallback.class);
        
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
        int i;

        for( i=0; i<10; i++) {
            final int loop = i;
            String topic="first_topic";
            String key= "id_"+Integer.toString(loop);
            String value= "Hello from java again>" + Integer.toString(loop);


            //create a topic
            ProducerRecord<String,String> record = new  ProducerRecord<String,String>(topic, key, value);
            logger.info("   Key value:       "+ key);

            //send the data
            producer.send(record, new Callback(){
                public void onCompletion(org.apache.kafka.clients.producer.RecordMetadata metadata, Exception exception) {
                    //executes everytime a record is success of fail
                    if (exception ==  null) {
                        logger.info("\n       Received new metadata for " + Integer.toString(loop) + "\n" +
                                    "       Topic       : " + metadata.topic() +"\n"+
                                    "       Partition   : " + metadata.partition() + "\n" +
                                    "       Offset      : " + metadata.offset() + "\n" +
                                    "       Time        : " + metadata.timestamp());
                    } 
                    else
                    {
                        logger.error("Error while producing record", exception);
                    }
                };
            } ).get();
        }
        producer.flush();
        producer.close();
    }
}
