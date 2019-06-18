package com.example.kafka.producer.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.producer.bean.Employee;
import com.example.kafka.producer.config.KafkaTopicCreation;

@RestController
@RequestMapping("/kafka")
public class EmployeeController
{

   @Autowired
   private KafkaTopicCreation kafkaTopic;

    @PostMapping("/publish/topic")
    public String post (@RequestBody Employee employee)
    {

        try {
            kafkaTopic.createTopic(employee.getName());
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error While Sending the topic");
        }
        kafkaTopic.listTopics();
        kafkaTopic.send(employee.getName(), employee);

        return "Topic " + employee.getName() + " Published successfully";
    }

}
