package com.example.rabbitmq.message.batch;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class MessageSender implements CommandLineRunner {

	private final RabbitTemplate rabbitTemplate;
	private final BatchMessageConsumer receiver;

	public MessageSender(BatchMessageConsumer receiver, RabbitTemplate rabbitTemplate) {
		this.receiver = receiver;
		this.rabbitTemplate = rabbitTemplate;
		this.rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter(new ObjectMapper()));
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println("Sending message...");
		HashMap<Object, Object> mailData = new HashMap<Object, Object>();
		mailData.put("email", "email@gmail.com");
		mailData.put("msg", "Hello");
		for (int i = 0; i < 10; i++) {
			rabbitTemplate.convertAndSend(MessagingRabbitmqApplication.directExchangeName, "my.batch", mailData);
			if(i%5 == 0)
			{
				Thread.sleep(500);
			}
		}
		
		receiver.getLatch().await(5000, TimeUnit.MILLISECONDS);
	}

}
