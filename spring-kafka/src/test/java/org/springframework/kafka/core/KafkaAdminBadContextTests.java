/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
public class KafkaAdminBadContextTests {

	@Test
	public void testContextNotLoaded() {
		assertThatIllegalStateException()
				.isThrownBy(() -> new AnnotationConfigApplicationContext(BadConfig.class).close())
				.withMessageMatching("(Could not create admin|Could not configure topics)");
	}

	@Configuration
	public static class BadConfig {

		@Bean
		public EmbeddedKafkaBroker kafkaEmbedded() {
			return new EmbeddedKafkaKraftBroker(1, 1);
		}

		@Bean
		public KafkaAdmin admin() {
			Map<String, Object> configs = new HashMap<>();
			configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "junk.host.ajshasdjasdk:1234");
			// use the following to get the "Could not configure topics" variant
			// can't be a CI test
//			configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
			configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "100");
			KafkaAdmin kafkaAdmin = new KafkaAdmin(configs);
			kafkaAdmin.setFatalIfBrokerNotAvailable(true);
			return kafkaAdmin;
		}

		@Bean
		public NewTopic topic1() {
			return new NewTopic("baz", 1, (short) 1);
		}

	}

}
