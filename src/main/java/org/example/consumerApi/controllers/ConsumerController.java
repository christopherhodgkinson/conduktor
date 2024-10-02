package org.example.consumerApi.controllers;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.consumerApi.dao.KafkaAdminDAO;
import org.example.consumerApi.dao.KafkaConfig;
import org.example.consumerApi.dao.KafkaConsumerDAO;
import org.example.consumerApi.service.ConsumerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping(path="/topic")
public class ConsumerController {

    @GetMapping(path="/{topicName}/{offset}")
    public ResponseEntity<String> getKafkaMessages(@PathVariable String topicName,
                                                   @PathVariable Integer offset,
                                                   @RequestParam(required = false)  Integer count
    ){
        // set default for count if not provided
        if (count == null){
            count = 10;
        }

        // process request
        ConsumerService consumerService = new ConsumerService(
                new KafkaConsumerDAO(KafkaConsumerDAO.consumerFactory()),
                new KafkaAdminDAO(KafkaAdminClient.create(KafkaConfig.getAdminConfig()))
        );
        List<ConsumerRecord<String, String>> outputList = consumerService.retrieveRecords(topicName, offset, count);

        // handle output format
        List<String> out = outputList.stream().map(rec -> rec.key() + "|"+rec.value() + "|" + rec.topic() + "-" + rec.partition() + "|" + rec.timestamp()).toList();
        return ResponseEntity.of(Optional.of( String.join(", ", out)));
    }

    @ControllerAdvice
    public static class GlobalExceptionHandler {
        @ExceptionHandler(Exception.class)
        public ResponseEntity<String> handleAllExceptions(Exception ex) {
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
