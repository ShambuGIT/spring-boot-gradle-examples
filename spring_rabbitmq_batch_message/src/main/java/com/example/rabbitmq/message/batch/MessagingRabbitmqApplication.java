package com.example.rabbitmq.message.batch;

import org.springframework.amqp.core.BatchMessageListener;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.StatelessRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class MessagingRabbitmqApplication {

	static final String directExchangeName = "amq.direct";

	static final String queueName = "my.batch.queue";

	@Bean
	Queue queue() {
		return new Queue(queueName, false);
	}

	@Bean
	DirectExchange exchange() {
		return new DirectExchange(directExchangeName);
	}

	@Bean
	Binding binding(Queue queue, DirectExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("my.batch");
	}

	@Bean
	SimpleMessageListenerContainer container(ConnectionFactory connectionFactory,
			BatchMessageListener consumerLisener) {

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setQueueNames(queueName);
		container.setMessageListener(consumerLisener);
		container.setPrefetchCount(3);
		container.setConsumerBatchEnabled(true);
		container.setReceiveTimeout(1000);
		container.setBatchSize(3);
		// Enable this for retry
//		container.setAdviceChain(createAdviceChain().getObject());
		return container;
	}

	// Enable this for retry
	@Bean
	StatelessRetryOperationsInterceptorFactoryBean createAdviceChain()
	{
		StatelessRetryOperationsInterceptorFactoryBean bean = new StatelessRetryOperationsInterceptorFactoryBean();
		RetryTemplate retry = new RetryTemplate();
		retry.setRetryPolicy(new SimpleRetryPolicy(10));
		bean.setRetryOperations(retry);
		return bean;
	}

	@Bean
	CustomBatchMessageListenerAdaptor listenerAdaptor(BatchMessageConsumer consumer,
			Jackson2JsonMessageConverter messageConverter) {
		return new CustomBatchMessageListenerAdaptor(consumer, messageConverter);
	}

	@Bean
	Jackson2JsonMessageConverter messageConverter() {
		return new Jackson2JsonMessageConverter(new ObjectMapper());
	}

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(MessagingRabbitmqApplication.class, args).close();
	}

}
