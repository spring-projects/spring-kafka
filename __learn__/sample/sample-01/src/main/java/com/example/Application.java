package com.example;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackoff;

import com.common.Foo2;

// demo dead letter topic
@SpringBootApplication
public class Application {

	private final Logger logger = LoggerFactory.getLogger(Application.class);

	private final TaskExecutor exec = new SimpleAsyncTaskExecutor();

	private final String instruction = "press Enter to stop this app!";

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args).close();
	}

	// this will be autowired into the container factory by Spring Boot
	@Bean
	public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
		return new DefaultErrorHandler(
			new DeadLetterPublishingRecoverer(template),
			new FixedBackoff(1000L, 2)
		);
	}

	@Bean
	public RecordMessageConverter converter() {
		return new JsonMessageConverter();
	}

	@KafkaListener(id = "fooGroup", topics = "topic1")
	public void listen(Foo2 foo) {
		logger.info("Received message: " + foo);
		if (foo.getFoo().startsWith("fail")) {
			throw new RuntimeException("whoops - failed!");
		}
		this.exec.execute(() -> System.out.println(instruction));
	}

	@KafkaListener(id = "dltGroup", topics = "topic1.DLT")
	public void dltListen(byte[] in) {
		logger.info("Received message from DLT: " + new String(in));
		this.exec.execute(() -> System.out.println(instruction));
	}

	@Bean
	public NewTopic topic() {
		return new Topic("topic1", 1, (short) 1);
	}

	@Bean
	public NewTopic dlt() {
		return new Topic("topic1.DLT", 1, (short) 1);
	}

	@Bean
	@Profile("default") // avoid running from test(s)
	public ApplicationRunner runner() {
		return args -> {
			System.out.println(instruction);
			System.in.read();
		};
	}
}
