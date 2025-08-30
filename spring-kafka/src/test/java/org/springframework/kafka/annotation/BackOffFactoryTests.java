/*
 * Copyright 2018-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.EmbeddedValueResolver;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Tests for {@link BackOffFactory}.
 *
 * @author Stephane Nicoll
 */
class BackOffFactoryTests {

	private final BackOffFactory backOffFactory = new BackOffFactory(null);

	@Test
	void createFromAnnotationWithDefaults() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withDefaults")))
				.isInstanceOfSatisfying(FixedBackOff.class, (backOff) ->
						assertThat(backOff.getInterval()).isEqualTo(1000));
	}

	@Test
	void createFromAnnotationWithDelayOnlyOnValue() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withDelayOnlyValue")))
				.isInstanceOfSatisfying(FixedBackOff.class, (backOff) ->
						assertThat(backOff.getInterval()).isEqualTo(2000));
	}

	@Test
	void createFromAnnotationWithDelayOnlyOnAttribute() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withDelayOnlyAttribute")))
				.isInstanceOfSatisfying(FixedBackOff.class, (backOff) ->
						assertThat(backOff.getInterval()).isEqualTo(2000));
	}

	@Test
	void createFromAnnotationWithDelayValueToZero() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withDelayValueToZero")))
				.isInstanceOfSatisfying(FixedBackOff.class, (backOff) ->
						assertThat(backOff.getInterval()).isEqualTo(0));
	}

	@Test
	void createFromAnnotationWithDelayAttributeToZero() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withDelayAttributeToZero")))
				.isInstanceOfSatisfying(FixedBackOff.class, (backOff) ->
						assertThat(backOff.getInterval()).isEqualTo(0));
	}

	@Test
	void createFromAnnotationWithInvalidDelay() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.backOffFactory.createFromAnnotation(getAnnotation("withInvalidDelay")))
				.withMessage("Invalid delay (-1ms): must be >= 0.");
	}

	@Test
	void createFromAnnotationWithInvalidMaxDelay() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.backOffFactory.createFromAnnotation(getAnnotation("withInvalidMaxDelay")))
				.withMessage("Invalid maxDelay (-1ms): must be greater than zero.");
	}

	@Test
	void createFromAnnotationWithInvalidMultiplier() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.backOffFactory.createFromAnnotation(getAnnotation("withInvalidMultiplier")))
				.withMessage("Invalid multiplier '-0.5': must be greater than or equal to 1. A multiplier of 1 is equivalent to a fixed delay.");
	}

	@Test
	void createFromAnnotationWithInvalidJitter() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.backOffFactory.createFromAnnotation(getAnnotation("withInvalidJitter")))
				.withMessage("Invalid jitter (-1ms): must be greater than or equal to zero.");
	}

	@Test
	void createFromAnnotationWithAttributeValues() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withValues")))
				.isInstanceOfSatisfying(ExponentialBackOff.class, (backOff) -> {
					assertThat(backOff.getInitialInterval()).isEqualTo(900);
					assertThat(backOff.getMaxInterval()).isEqualTo(4000);
					assertThat(backOff.getMultiplier()).isEqualTo(1.7);
					assertThat(backOff.getJitter()).isEqualTo(500);
				});
	}

	@Test
	void createFromAnnotationWithDurationFormat() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withDurationFormat")))
				.isInstanceOfSatisfying(ExponentialBackOff.class, (backOff) -> {
					assertThat(backOff.getInitialInterval()).isEqualTo(900);
					assertThat(backOff.getMaxInterval()).isEqualTo(4000);
					assertThat(backOff.getMultiplier()).isEqualTo(1.7);
					assertThat(backOff.getJitter()).isEqualTo(500);
				});
	}

	@Test
	void createFromAnnotationWithDelayAndDelayString() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withDelayAndDelayString")))
				.isInstanceOfSatisfying(FixedBackOff.class, (backOff) ->
						assertThat(backOff.getInterval()).isEqualTo(4000));
	}

	@Test
	void createFromAnnotationWithMaxDelayAndMaxDelayString() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withMaxDelayAndMaxDelayString")))
				.isInstanceOfSatisfying(ExponentialBackOff.class, (backOff) -> {
					assertThat(backOff.getMaxInterval()).isEqualTo(4000);
				});
	}

	@Test
	void createFromAnnotationWithMultiplierAndMultiplierString() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withMultiplierAndMultiplierString")))
				.isInstanceOfSatisfying(ExponentialBackOff.class, (backOff) -> {
					assertThat(backOff.getMultiplier()).isEqualTo(2.5);
				});
	}

	@Test
	void createFromAnnotationWithJitterAndJitterString() {
		assertThat(this.backOffFactory.createFromAnnotation(getAnnotation("withJitterAndJitterString")))
				.isInstanceOfSatisfying(ExponentialBackOff.class, (backOff) -> {
					assertThat(backOff.getJitter()).isEqualTo(1000);
				});
	}

	@Test
	void createFromAnnotationWithStringProperties() {
		MockEnvironment environment = new MockEnvironment()
				.withProperty("test.delay", "2s")
				.withProperty("test.max-delay", "4s")
				.withProperty("test.multiplier", "1.6")
				.withProperty("test.jitter", "500");
		assertThat(createFromAnnotation(getAnnotation("withStringProperties"), environment))
				.isInstanceOfSatisfying(ExponentialBackOff.class, (backOff) -> {
					assertThat(backOff.getInitialInterval()).isEqualTo(2000);
					assertThat(backOff.getMaxInterval()).isEqualTo(4000);
					assertThat(backOff.getMultiplier()).isEqualTo(1.6);
					assertThat(backOff.getJitter()).isEqualTo(500);
				});
	}

	@Test
	void createFromAnnotationWithStringSpEL() {
		MockEnvironment environment = new MockEnvironment()
				.withProperty("test.jitter", "500");
		assertThat(createFromAnnotation(getAnnotation("withStringSpEL"), environment))
				.isInstanceOfSatisfying(ExponentialBackOff.class, (backOff) -> {
					assertThat(backOff.getInitialInterval()).isEqualTo(2000);
					assertThat(backOff.getMaxInterval()).isEqualTo(4000);
					assertThat(backOff.getMultiplier()).isEqualTo(1.6);
					assertThat(backOff.getJitter()).isEqualTo(500);
				});
	}

	private org.springframework.util.backoff.BackOff createFromAnnotation(BackOff annotation, ConfigurableEnvironment environment) {
		StaticApplicationContext context = new StaticApplicationContext();
		context.setEnvironment(environment);
		context.refresh();
		return new BackOffFactory(new EmbeddedValueResolver(context.getBeanFactory())).createFromAnnotation(annotation);
	}

	private BackOff getAnnotation(String methodName) {
		Method method = ReflectionUtils.findMethod(Sample.class, methodName);
		assertThat(method).isNotNull();
		return method.getAnnotation(TestAnnotation.class).backOff();
	}

	@SuppressWarnings("unused")
	static class Sample {

		@TestAnnotation(backOff = @BackOff)
		void withDefaults() {
		}

		@TestAnnotation(backOff = @BackOff(2000))
		void withDelayOnlyValue() {
		}

		@TestAnnotation(backOff = @BackOff(delay = 2000))
		void withDelayOnlyAttribute() {
		}

		@TestAnnotation(backOff = @BackOff(0))
		void withDelayValueToZero() {
		}

		@TestAnnotation(backOff = @BackOff(0))
		void withDelayAttributeToZero() {
		}

		@TestAnnotation(backOff = @BackOff(-1))
		void withInvalidDelay() {
		}

		@TestAnnotation(backOff = @BackOff(maxDelay = -1))
		void withInvalidMaxDelay() {
		}

		@TestAnnotation(backOff = @BackOff(multiplier = -0.5))
		void withInvalidMultiplier() {
		}

		@TestAnnotation(backOff = @BackOff(jitter = -1))
		void withInvalidJitter() {
		}

		@TestAnnotation(backOff = @BackOff(delay = 900, maxDelay = 4000, multiplier = 1.7, jitter = 500))
		void withValues() {
		}

		@TestAnnotation(backOff = @BackOff(delayString = "900ms", maxDelayString = "4s", multiplierString = "1.7", jitterString = "500ms"))
		void withDurationFormat() {
		}

		@TestAnnotation(backOff = @BackOff(delay = 2000, delayString = "4s"))
		void withDelayAndDelayString() {
		}

		@TestAnnotation(backOff = @BackOff(maxDelay = 2000, maxDelayString = "4s"))
		void withMaxDelayAndMaxDelayString() {
		}

		@TestAnnotation(backOff = @BackOff(multiplier = 1.5, multiplierString = "2.5"))
		void withMultiplierAndMultiplierString() {
		}

		@TestAnnotation(backOff = @BackOff(jitter = 500, jitterString = "1s"))
		void withJitterAndJitterString() {
		}

		@TestAnnotation(backOff = @BackOff(delayString = "${test.delay}", maxDelayString = "${test.max-delay}", multiplierString = "${test.multiplier}", jitterString = "${test.jitter}"))
		void withStringProperties() {
		}

		@TestAnnotation(backOff = @BackOff(delayString = "#{2 * 1000}", maxDelayString = "#{8000 / 2}", multiplierString = "#{0.16 * 10}", jitterString = "#{environment.getProperty('test.jitter')}"))
		void withStringSpEL() {
		}

	}

	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	@interface TestAnnotation {

		BackOff backOff();
	}

}
