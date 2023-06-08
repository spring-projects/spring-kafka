package org.springframework.kafka.test.condition;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.condition.WithNestedClassContextTests.Config;

@EmbeddedKafka
@SpringJUnitConfig(Config.class)
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

	public static class TestClass {
	}

	@Configuration
	static class Config {
		@Bean
		public TestClass testClass() {
			counter.incrementAndGet();
			return new TestClass();
		}
	}
}
