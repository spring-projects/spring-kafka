/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.8.4
 *
 */
public class ExceptionClassifierTests {

	@Test
	void testDefault() {
		ExceptionClassifier ec = new ExceptionClassifier() {
		};
		assertThat(ec.getExceptionMatcher().match(new Exception())).isTrue();
		assertThat(ec.getExceptionMatcher().match(new ClassCastException())).isFalse();
		ec.removeClassification(ClassCastException.class);
		assertThat(ec.getExceptionMatcher().match(new ClassCastException())).isTrue();
		assertThat(ec.getExceptionMatcher().match(new IllegalStateException())).isTrue();
		ec.addNotRetryableExceptions(IllegalStateException.class);
		assertThat(ec.getExceptionMatcher().match(new IllegalStateException())).isFalse();
	}

	@Test
	void testDefaultFalse() {
		ExceptionClassifier ec = new ExceptionClassifier() {
		};
		assertThat(ec.getExceptionMatcher().match(new Exception())).isTrue();
		ec.defaultFalse();
		assertThat(ec.getExceptionMatcher().match(new Exception())).isFalse();
		assertThat(ec.getExceptionMatcher().match(new IllegalStateException())).isFalse();
		ec.addRetryableExceptions(IllegalStateException.class);
		assertThat(ec.getExceptionMatcher().match(new IllegalStateException())).isTrue();
		ec.removeClassification(IllegalStateException.class);
		assertThat(ec.getExceptionMatcher().match(new IllegalStateException())).isFalse();
	}

}
