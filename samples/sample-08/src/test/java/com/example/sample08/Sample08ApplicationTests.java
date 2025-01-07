package com.example.sample08;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import brave.handler.SpanHandler;
import brave.test.TestSpanHandler;
import io.micrometer.tracing.brave.bridge.BraveFinishedSpan;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1)
@AutoConfigureObservability
class Sample08ApplicationTests {

	@Test
	void verifyObservationIsPropagatedFromProduceToConsumer(@Autowired TestSpanHandler testSpanHandler) {
		await().atMost(Duration.ofSeconds(30))
				.untilAsserted(() -> assertThat(testSpanHandler.spans()).hasSize(2));
		SpansAssert.assertThat(testSpanHandler.spans().stream().map(BraveFinishedSpan::fromBrave).toList())
				.haveSameTraceId();
	}

	@TestConfiguration
	public static class AdditionalTestBeansConfiguration {

		@Bean
		TestSpanHandler testSpanHandler() {
			return new TestSpanHandler();
		}

	}

}
