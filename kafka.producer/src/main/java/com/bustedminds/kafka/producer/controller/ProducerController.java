package com.bustedminds.kafka.producer.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bustedminds.kafka.producer.DTO.Student;
import com.google.gson.Gson;

@RestController
@RequestMapping("/produce")
public class ProducerController {
	
	@Autowired
	private KafkaTemplate<String,String> kafkaTemplate;
	
	@Autowired
	private Gson gson;
	
	@PostMapping("/newStudent")
	public ResponseEntity<String> createNewStudent(@RequestBody Student student) 
			throws InterruptedException, ExecutionException{
		ListenableFuture<SendResult<String,String>> result = kafkaTemplate.send("new-student",gson.toJson(student));
		return new ResponseEntity<>(result.get().getProducerRecord().value(), HttpStatus.OK);
	}

}
