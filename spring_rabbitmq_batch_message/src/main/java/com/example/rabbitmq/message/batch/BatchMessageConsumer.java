package com.example.rabbitmq.message.batch;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class BatchMessageConsumer {

	private CountDownLatch latch = new CountDownLatch(1);

	public void handleMessages(List<Message<?>> messages) {
		System.out.println("No of messages are recevied = " + messages.size());
		int i = 1;
		for (Message<?> message : messages) {
			System.out.println("Msg " + i++ + " : " + message);
		}
		// Just pause for secs to watch queue processing in rabbitme console.
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Enable this for retry.
//		throw new RuntimeException("Retry...");
	}

	public CountDownLatch getLatch() {
		return latch;
	}

}
