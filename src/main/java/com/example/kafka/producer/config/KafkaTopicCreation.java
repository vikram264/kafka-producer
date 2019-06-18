package com.example.kafka.producer.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.kafka.producer.bean.Employee;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaTopicCreation
{
    private AdminClient adminClient;

    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;

    public String createTopic (String topicName)
        throws InterruptedException, ExecutionException
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        adminClient = AdminClient.create(props);
        NewTopic newTopic = new NewTopic(topicName, 1, (short)1);
        adminClient.createTopics(Collections.singleton(newTopic));
        String message = "Created a topic with name ::" + newTopic.name();
        log.info(message);
        return message;
    }

    public void listTopics ()
    {
        ListTopicsResult ltr = adminClient.listTopics();
        log.info("All Topics Present :: " + ltr.namesToListings()
            .toString());
    }

    public void send (String topicName, Employee employee)
    {
        kafkaTemplate.send(topicName, employee);
    }
}
