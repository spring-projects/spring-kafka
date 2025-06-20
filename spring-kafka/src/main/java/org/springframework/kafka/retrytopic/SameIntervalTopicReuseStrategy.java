/*
 * Copyright 2021-present the original author or authors.
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
 * Strategy for topic reuse when multiple, sequential retries have the same backoff
 * interval.
 *
 * <p>
 * It can be used only when the retries that have the same interval are located
 * in the end of the retry chain (it cannot be used for retries with the same
 * interval in the middle of the retry chain).
 *
 * @author João Lima
 * @since 3.0.4
 *
 */
public enum SameIntervalTopicReuseStrategy {

	/**
	 * Uses a single retry topic for sequential attempts with the same interval.
	 */
	SINGLE_TOPIC,

	/**
	 * Uses one separate topic per retry attempt.
	 */
	MULTIPLE_TOPICS

}
