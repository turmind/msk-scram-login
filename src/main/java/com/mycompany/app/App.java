package com.mycompany.app;

import java.time.Duration;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

//Create java class named “SimpleProducer”
public class App {
   
   public static void main(String[] args) throws Exception{

      // 更换用户名。密码
      String username = "xxx";
      String password = "xxx-secret";
      //Assign topicName to string variable
      // 更换topic
      String topicName = "MSKTutorialTopic";
      
      // create instance for properties to access producer configs   
      Properties props = new Properties();
      
      //Assign localhost id, change the $endpoint to msk endpoint
      props.put("bootstrap.servers", "b-2-public..amazonaws.com:9196,b-1-public..amazonaws.com:9196"); //修改这行
      
      //Set acknowledgements for producer requests.
      props.put("acks", "all");
      
      //If the request fails, the producer can automatically retry,
      props.put("retries", 0);
      
      //Specify buffer size in config
      props.put("batch.size", 16384);
      
      //Reduce the no of requests less than 0   
      props.put("linger.ms", 1);
      
      //The buffer.memory controls the total amount of memory available to the producer for buffering.   
      props.put("buffer.memory", 33554432);
      
      props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
         
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");

      props.put("security.protocol", "SASL_SSL");
      props.put("sasl.mechanism", "SCRAM-SHA-512");
      props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
      
      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);
            
      for(int i = 0; i < 10; i++){
         producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
         System.out.println("Message sent successfully");
      }
      producer.close();

      props.put("group.id", "test");
      props.put("key.deserializer", StringDeserializer.class.getName());
      props.put("value.deserializer", StringDeserializer.class.getName());
      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
      consumer.subscribe(java.util.Collections.singletonList(topicName));
      for(int i = 0; i < 10; i++){
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1, 0));
         for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s%n\n", record.offset(), record.key(), record.value());
         }
      }
      consumer.close();
    }
}