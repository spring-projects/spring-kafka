/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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
