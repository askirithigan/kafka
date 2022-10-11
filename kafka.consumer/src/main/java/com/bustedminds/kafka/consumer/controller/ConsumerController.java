package com.bustedminds.kafka.consumer.controller;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bustedminds.kafka.consumer.DTO.Student;
import com.google.gson.Gson;

@RestController
@RequestMapping("/consumer")
public class ConsumerController {
	
	@Autowired
	private Gson gson;
	
	private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);
	
	@KafkaListener(topics= "#{'${topic.name}'}")
	public void newStudents(@RequestBody String student) {
		log.info("Kafka Event Consumed is:"+student);
		Student newStudent = gson.fromJson(student, Student.class);
		log.info("Converted Value:"+newStudent.toString());
	}
	
}
