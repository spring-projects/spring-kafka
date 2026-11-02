/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.9.3
 *
 */
public class ErrorHandlingCoverageTests {

	@Test
	void coverageDefault() {
		DefaultErrorHandler eh = new DefaultErrorHandler();
		eh.addNotRetryableExceptions(IllegalStateException.class);
		eh.setClassifications(Map.of(IllegalArgumentException.class, false), false);
		assertThat(eh.removeClassification(IllegalStateException.class)).isNull();
		assertThat(eh.removeClassification(IllegalArgumentException.class)).isFalse();
	}

}
