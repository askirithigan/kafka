package com.bustedminds.kafka.consumer.controller;

import org.springframework.beans.factory.annotation.Autowired;
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
	
	@KafkaListener(topics= {"new-student"})
	public void newStudents(@RequestBody String student) {
		System.out.println("Kafka Event Consumed is:"+student);
		Student newStudent = gson.fromJson(student, Student.class);
		System.out.println("Converted Value:"+newStudent.toString());
		
	}

}
