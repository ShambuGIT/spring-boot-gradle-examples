package com.example.rabbitmq.message.batch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.core.BatchMessageListener;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.util.MethodInvoker;

public class CustomBatchMessageListenerAdaptor implements BatchMessageListener {

	private MessagingMessageConverter convertor;

	private Object delegate;

	private String methodName = "handleMessages";

	public CustomBatchMessageListenerAdaptor(Object delegate, MessageConverter messageConverter) {
		this.delegate = delegate;
		this.convertor = new MessagingMessageConverter();
		setMessageConverter(messageConverter);
	}

	public void setMessageConverter(MessageConverter messageConverter) {
		this.convertor.setPayloadConverter(messageConverter);
	}

	@Override
	public void onMessageBatch(List<org.springframework.amqp.core.Message> originalMessage) {
		List<Message<?>> messagingMessages = new ArrayList<>();
		for (org.springframework.amqp.core.Message amqpMessage : originalMessage) {
			messagingMessages.add((Message<?>) convertor.fromMessage(amqpMessage));
		}

		try {
			MethodInvoker methodInvoker = new MethodInvoker();
			methodInvoker.setTargetObject(delegate);
			methodInvoker.setTargetMethod(methodName);
			methodInvoker.setArguments(messagingMessages);
			methodInvoker.prepare();
			methodInvoker.invoke();
		} catch (Exception ex) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
		}
	}
}
