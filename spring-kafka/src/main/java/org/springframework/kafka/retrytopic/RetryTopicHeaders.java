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
 * Contains the headers that will be used in the forwarded messages.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public abstract class RetryTopicHeaders {

	/**
	 * The default header for the backoff due timestamp.
	 */
	public static final String DEFAULT_HEADER_BACKOFF_TIMESTAMP = "retry_topic-backoff-timestamp";

	/**
	 * The default header for the attempts.
	 */
	public static final String DEFAULT_HEADER_ATTEMPTS = "retry_topic-attempts";

	/**
	 * The default header for the original message's timestamp.
	 */
	public static final String DEFAULT_HEADER_ORIGINAL_TIMESTAMP = "retry_topic-original-timestamp";

}
