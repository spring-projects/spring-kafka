/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
class RetryTopicConstantsTests {

	private static final String DEFAULT_RETRY_SUFFIX = "-retry";

	private static final String DEFAULT_DLT_SUFFIX = "-dlt";

	private static final int DEFAULT_MAX_ATTEMPTS = 3;

	private static final int NOT_SET = -1;

	@Test
	public void assertRetryTopicConstants() {
		new RetryTopicConstants() { }; // for coverage
		assertThat(RetryTopicConstants.DEFAULT_DLT_SUFFIX).isEqualTo(DEFAULT_DLT_SUFFIX);
		assertThat(RetryTopicConstants.DEFAULT_RETRY_SUFFIX).isEqualTo(DEFAULT_RETRY_SUFFIX);
		assertThat(RetryTopicConstants.DEFAULT_MAX_ATTEMPTS).isEqualTo(DEFAULT_MAX_ATTEMPTS);
		assertThat(RetryTopicConstants.NOT_SET).isEqualTo(NOT_SET);
	}
}
