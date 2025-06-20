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

package org.springframework.kafka.retrytopic;

/**
 *
 * Constants for the RetryTopic functionality.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public abstract class RetryTopicConstants {

	/**
	 * Default suffix for retry topics.
	 */
	public static final String DEFAULT_RETRY_SUFFIX = "-retry";

	/**
	 * Default suffix for dlt.
	 */
	public static final String DEFAULT_DLT_SUFFIX = "-dlt";

	/**
	 * Default number of times the message processing should be attempted,
	 * including the first.
	 */
	public static final int DEFAULT_MAX_ATTEMPTS = 3;

	/**
	 * Constant to represent that the integer property is not set.
	 */
	public static final int NOT_SET = -1;

}
