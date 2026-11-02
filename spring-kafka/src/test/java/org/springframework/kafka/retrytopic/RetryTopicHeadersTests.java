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
class RetryTopicHeadersTests {

	private static final String DEFAULT_HEADER_BACKOFF_TIMESTAMP = "retry_topic-backoff-timestamp";

	private static final String DEFAULT_HEADER_ATTEMPTS = "retry_topic-attempts";

	private static final String DEFAULT_HEADER_ORIGINAL_TIMESTAMP = "retry_topic-original-timestamp";

	@Test
	public void assertRetryTopicHeadersConstants() {
		new RetryTopicHeaders() { }; // for coverage
		assertThat(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP).isEqualTo(DEFAULT_HEADER_BACKOFF_TIMESTAMP);
		assertThat(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS).isEqualTo(DEFAULT_HEADER_ATTEMPTS);
		assertThat(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP).isEqualTo(DEFAULT_HEADER_ORIGINAL_TIMESTAMP);
	}
}
