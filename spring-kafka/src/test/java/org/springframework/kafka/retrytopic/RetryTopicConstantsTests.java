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
