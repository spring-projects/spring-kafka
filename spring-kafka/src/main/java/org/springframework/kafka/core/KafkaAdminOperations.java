/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.jspecify.annotations.Nullable;

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
	 * Delete topics from the Kafka cluster.
	 * @param topicNames the topic names to delete.
	 * @since 4.0
	 */
	void deleteTopics(String... topicNames);

	/**
	 * Return the cluster id, if available.
	 * @return the describe cluster id.
	 * @since 3.0
	 */
	@Nullable
	String clusterId();

}
