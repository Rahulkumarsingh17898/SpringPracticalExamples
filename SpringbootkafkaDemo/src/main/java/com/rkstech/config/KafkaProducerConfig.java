package com.rkstech.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerConfig
{
    @Bean
    public NewTopic createNewTopic(){
        return new NewTopic("rkstechie-serialize-3", 3, (short) 1);
    }

}
