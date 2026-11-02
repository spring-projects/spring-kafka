/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.test.condition;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Lee Jaeheon
 * @author Soby Chacko
 */
@EmbeddedKafka
@SpringJUnitConfig(WithNestedClassContextTests.Config.class)
@DirtiesContext
class WithNestedClassContextTests {

	private static final AtomicInteger counter = new AtomicInteger();

	@Autowired
	private TestClass outer;

	@Nested
	class NestedClass {

		@Test
		void equalsInjected(@Autowired TestClass inner) {
			assertThat(inner).isEqualTo(outer);
		}

		@Test
		void equalsSize(@Autowired List<TestClass> classes) {
			assertThat(classes).hasSize(1);
		}

		@Test
		void equalsCount() {
			assertThat(counter.get()).isEqualTo(1);
		}

	}

	public static class TestClass { }

	@Configuration
	static class Config {

		@Bean
		public TestClass testClass() {
			counter.incrementAndGet();
			return new TestClass();
		}

	}

}
