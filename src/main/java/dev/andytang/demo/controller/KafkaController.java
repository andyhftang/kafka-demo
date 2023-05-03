package dev.andytang.demo.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @PostMapping("/topics")
    public ResponseEntity<?> health(@RequestBody Map<String, Object> requestBody) throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(requestBody);
        ListTopicsResult result = adminClient.listTopics();
        return new ResponseEntity(result.names().get(), HttpStatus.OK);
    }

    @PostMapping("/produce")
    public ResponseEntity<?> produce(@RequestBody Map<String, Object> requestBody) {
        Producer<String, String> producer = new KafkaProducer<>(requestBody);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>((String) requestBody.get("topic"), (String) requestBody.get("value"));
        producer.send(producerRecord);
        return new ResponseEntity("OK", HttpStatus.OK);
    }

}
