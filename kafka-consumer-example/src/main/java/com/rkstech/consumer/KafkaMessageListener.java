package com.rkstech.consumer;

import com.rkstech.dto.Customer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class KafkaMessageListener {
    Logger logger = Logger.getLogger(KafkaMessageListener.class.getName());

//    since producer is sending message of type String consumer should also send message of type string

//    This KafkaListener annotation informs the consumer which topic it need to consume the data from

//    we can give any unique name to consumer group  it is to identify which task it is doing and inside a consumer group there could be multiple consumers which work upon different partition for a topic

//jt-group-1: partitions assigned: [rkstechie-custom-java-topic-2-0, rkstechie-custom-java-topic-2-1, rkstechie-custom-java-topic-2-2, rkstechie-custom-java-topic-2-3, rkstechie-custom-java-topic-2-4]
//    as we can see a single consumer group has one instance which is reading from all the partition

//    also we will change the group id and topic name for safer side as we previously consumed form this jt-group-1
    @KafkaListener(topics = "rkstechie-serialize-3", groupId = "jt-group")
    public void consumeEvents(Customer customer){
        logger.info("Consumer Consuming Events {} : " + customer.toString());
    }



    /*
    // now this way we can create multiple instacnces
    @KafkaListener(topics = "rkstechie-custom-java-topic-3", groupId = "jt-group")
    public void consume2(String message){
        logger.info("Consumer2 Consuming Message: " + message);
    }

    @KafkaListener(topics = "rkstechie-custom-java-topic-3", groupId = "jt-group")
    public void consume3(String message){
        logger.info("Consumer3 Consuming Message: " + message);
    }

    @KafkaListener(topics = "rkstechie-custom-java-topic-3", groupId = "jt-group")
    public void consume4(String message){
        logger.info("Consumer4 Consuming Message: " + message);
    }

    @KafkaListener(topics = "rkstechie-custom-java-topic-3", groupId = "jt-group")
    public void consume5(String message){
        logger.info("Consumer5 Consuming Message: " + message);
    }

//backup consumer
    @KafkaListener(topics = "rkstechie-custom-java-topic-3", groupId = "jt-group")
    public void consumeBackup(String message){
        logger.info("Consumer Backup Consuming Message: " + message);
    }

//    In real systems we should not write the code like this as we should use better concurrency to optimize consumers this is for demo purpose

*/

}
