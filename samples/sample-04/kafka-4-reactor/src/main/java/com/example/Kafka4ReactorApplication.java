package com.example;

import java.lang.reflect.Method;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.listener.reactive.ReactorAdapter;
import org.springframework.stereotype.Component;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderResult;

@SpringBootApplication
public class Kafka4ReactorApplication {

	public static void main(String[] args) {
		SpringApplication.run(Kafka4ReactorApplication.class, args);
	}

	@Bean
	public ReactorAdapter adapter(Listener listener, KafkaProperties properties)
			throws NoSuchMethodException, SecurityException {

		// This will be done by the bean post processor
		Method method = Listener.class.getDeclaredMethod("listen", Flux.class);
		ReactorAdapter adapter = new ReactorAdapter(listener, method, "skReactorTopic");
		adapter.setConfigs(properties.buildConsumerProperties());
		return adapter;
	}

	@Bean
	public ReactiveKafkaProducerTemplate<String, String> template(KafkaProperties properties) {
		SenderOptions<String, String> senderOptions = SenderOptions.create(properties.buildProducerProperties());
		return new ReactiveKafkaProducerTemplate<>(senderOptions);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("skReactorTopic").partitions(1).replicas(1).build();
	}

	@Bean
	public ApplicationRunner runner(ReactiveKafkaProducerTemplate<String, String> template) {
		return args -> IntStream.range(0, 10).forEach(i -> {
			Mono<SenderResult<Void>> send = template.send("skReactorTopic", "foo", "bar" + i);
			send.subscribe(sr -> System.out.println(sr.recordMetadata()));
		});
	}

}

@Component
class Listener {

	@KafkaListener(topics = "skReactorTopic")
	public Disposable listen(Flux<ReceiverRecord<String, String>> flux) {
		return flux.subscribe(record -> {
			System.out.println(record.key() + ":" + record.value() + "@" + record.offset());
			record.receiverOffset().acknowledge();
		});
	}

}

