/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.kafka.core;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import org.springframework.lang.Nullable;

/**
 * Provides a number of convenience methods wrapping {@code AdminClient}.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
public interface KafkaAdminOperations {

	/**
	 * Get an unmodifiable copy of this admin's configuration.
	 * @return the configuration map.
	 */
	Map<String, Object> getConfigurationProperties();

	/**
	 * Create topics if they don't exist or increase the number of partitions if needed.
	 * @param topics the topics.
	 */
	void createOrModifyTopics(NewTopic... topics);

	/**
	 * Obtain {@link TopicDescription}s for these topics.
	 * @param topicNames the topic names.
	 * @return a map of name:topicDescription.
	 */
	Map<String, TopicDescription> describeTopics(String... topicNames);

	/**
	 * Return the cluster id, if available.
	 * @return the describe cluster id.
	 * @since 3.0
	 */
	@Nullable
	String clusterId();

}
