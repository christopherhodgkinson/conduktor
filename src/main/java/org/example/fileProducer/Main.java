package org.example.fileProducer;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.example.consumerApi.dao.KafkaAdminDAO;
import org.example.consumerApi.dao.KafkaConfig;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static String TOPIC_NAME = "test_002";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        JSONParser parser = new JSONParser();
        JSONArray jsonArray;
        JSONObject jsonRoot;
        try {
            jsonRoot = (JSONObject) parser.parse(new FileReader("./src/main/resources/random-people-data.json"));
            jsonArray = (JSONArray) jsonRoot.get("ctRoot");
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }

        KafkaAdminDAO kafkaAdminDAO = new KafkaAdminDAO(KafkaAdminClient.create(KafkaConfig.getAdminConfig()));

        NewTopic newTopic = new NewTopic(TOPIC_NAME, 3, (short) 1);
        kafkaAdminDAO.createTopic(newTopic, TopicConfig.CLEANUP_POLICY_DELETE);

        KafkaProducer<String, String> producer = KafkaProducerDAO.getProducer();

        log.info("Sending "+jsonArray.size() + " messages");

        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            String key = (String) jsonObject.get("_id");
            String jsonStr = jsonObject.toJSONString();


            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, jsonStr);
            producer.send(record);
        }

        log.info("Sent "+jsonArray.size() + " messages");

        producer.close();

    }
}