package com.rkstech.service;

import com.rkstech.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    //to publish a mesage from application we need to use some class to talk to kafka that class is KafkaTemplate
    @Autowired
    private KafkaTemplate<String, Object> kafkaEvents;

//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessageToTopic(String message){
        //whenever we allow sb to create topic on behalf of us it will use the default config and create a topic



//        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("rkstechie-demo1", message);

        // for create a topic with custom config we need to create the topic manually from terminal using the command
//        rkstechie-custom-topc -> this i created manually from terminal with three partition

//        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("rkstechie-custom-topc", message);
// this was custom topic created from terminal there is a way from SB also that we can create a topic with custom config
//rkstechie-custom-java-topic => from kafkaProducerConfig file
//
        CompletableFuture<SendResult<String, Object>> future = kafkaEvents.send("rkstechie-custom-java-topic-3", message);


        // if here we use future.get the thread will wait for the result and slow down the producer
//        as kaka is a fast event streaming platform thus we should handle the request asynchronously by using a callback methot

        future.whenComplete((result,ex) -> {

            if (ex == null) {
                System.out.println("sent message=["+ message +"] with offset"+result.getRecordMetadata().offset()+ "with partition nu:" + result.getRecordMetadata().partition() +"]");
            }
            else  {
                System.out.println("unable to send message =[" + message +"] with offset"+result.getRecordMetadata().offset()+ "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendEventsToTopic(Customer customer){
        try {


            CompletableFuture<SendResult<String, Object>> future = kafkaEvents.send("rkstechie-serialize-3", customer);

            future.whenComplete((result, ex) -> {

                if (ex == null) {
                    System.out.println("sent message=[" + customer + "] with offset" + result.getRecordMetadata().offset() + "with partition nu:" + result.getRecordMetadata().partition() + "]");
                } else {
                    System.out.println("unable to send message =[" + customer + "] with offset" + result.getRecordMetadata().offset() + "] due to : " + ex.getMessage());
                }
            });

        } catch (Exception ex){
            System.out.println("Error while sending events to topic " + ex.getMessage());
        }

    }
}
