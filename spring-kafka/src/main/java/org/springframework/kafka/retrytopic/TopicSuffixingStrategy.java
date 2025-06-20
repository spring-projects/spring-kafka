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
public enum TopicSuffixingStrategy {

	/**
	 * Suffixes the topics with their index in the retry topics.
	 * E.g. my-retry-topic-0, my-retry-topic-1, my-retry-topic-2.
	 */
	SUFFIX_WITH_INDEX_VALUE,

	/**
	 * Suffixes the topics the delay value for the topic.
	 * E.g. my-retry-topic-1000, my-retry-topic-2000, my-retry-topic-4000.
	 */
	SUFFIX_WITH_DELAY_VALUE

}
