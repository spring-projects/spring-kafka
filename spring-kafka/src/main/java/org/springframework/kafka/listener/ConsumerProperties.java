/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;

import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Common consumer properties.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class ConsumerProperties {

	/**
	 * The default {@link #setPollTimeout(long) pollTimeout} (ms).
	 */
	public static final long DEFAULT_POLL_TIMEOUT = 5_000L;

	/**
	 * Topic names.
	 */
	private final String[] topics;

	/**
	 * Topic pattern.
	 */
	private final Pattern topicPattern;

	/**
	 * Topics/partitions/initial offsets.
	 */
	private final TopicPartitionOffset[] topicPartitionsToAssign;

	/**
	 * The max time to block in the consumer waiting for records.
	 */
	private long pollTimeout = DEFAULT_POLL_TIMEOUT;

	/**
	 * Override the group id.
	 */
	private String groupId;

	/**
	 * Override the client id.
	 */
	private String clientId = "";

	/**
	 * A user defined {@link ConsumerRebalanceListener} implementation.
	 */
	private ConsumerRebalanceListener consumerRebalanceListener;

	private Duration syncCommitTimeout;

	/**
	 * The commit callback; by default a simple logging callback is used to log
	 * success at DEBUG level and failures at ERROR level.
	 */
	private OffsetCommitCallback commitCallback;

	/**
	 * Whether or not to call consumer.commitSync() or commitAsync() when the
	 * container is responsible for commits. Default true.
	 */
	private boolean syncCommits = true;

	private LogIfLevelEnabled.Level commitLogLevel = LogIfLevelEnabled.Level.DEBUG;

	private Properties kafkaConsumerProperties = new Properties();

	/**
	 * Create properties for a container that will subscribe to the specified topics.
	 * @param topics the topics.
	 */
	public ConsumerProperties(String... topics) {
		Assert.notEmpty(topics, "An array of topics must be provided");
		this.topics = topics.clone();
		this.topicPattern = null;
		this.topicPartitionsToAssign = null;
	}

	/**
	 * Create properties for a container that will subscribe to topics matching the
	 * specified pattern. The framework will create a container that subscribes to all
	 * topics matching the specified pattern to get dynamically assigned partitions. The
	 * pattern matching will be performed periodically against topics existing at the time
	 * of check.
	 * @param topicPattern the pattern.
	 * @see org.apache.kafka.clients.CommonClientConfigs#METADATA_MAX_AGE_CONFIG
	 */
	public ConsumerProperties(Pattern topicPattern) {
		this.topics = null;
		this.topicPattern = topicPattern;
		this.topicPartitionsToAssign = null;
	}

	/**
	 * Create properties for a container that will assign itself the provided topic
	 * partitions.
	 * @param topicPartitions the topic partitions.
	 */
	public ConsumerProperties(TopicPartitionOffset... topicPartitions) {
		this.topics = null;
		this.topicPattern = null;
		Assert.notEmpty(topicPartitions, "An array of topicPartitions must be provided");
		this.topicPartitionsToAssign = Arrays.copyOf(topicPartitions, topicPartitions.length);
	}

	@Nullable
	public String[] getTopics() {
		return this.topics != null
				? Arrays.copyOf(this.topics, this.topics.length)
				: null;
	}

	@Nullable
	public Pattern getTopicPattern() {
		return this.topicPattern;
	}

	@Nullable
	public TopicPartitionOffset[] getTopicPartitionsToAssign() {
		return this.topicPartitionsToAssign != null
				? Arrays.copyOf(this.topicPartitionsToAssign, this.topicPartitionsToAssign.length)
				: null;
	}

	/**
	 * Set the max time to block in the consumer waiting for records.
	 * @param pollTimeout the timeout in ms; default {@value #DEFAULT_POLL_TIMEOUT}.
	 */
	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	public long getPollTimeout() {
		return this.pollTimeout;
	}

	/**
	 * Set the group id for this container. Overrides any {@code group.id} property
	 * provided by the consumer factory configuration.
	 * @param groupId the group id.
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getGroupId() {
		return this.groupId;
	}

	/**
	 * Return the client id.
	 * @return the client id.
	 * @see #setClientId(String)
	 */
	public String getClientId() {
		return this.clientId;
	}

	/**
	 * Set the client id; overrides the consumer factory client.id property.
	 * When used in a concurrent container, will be suffixed with '-n' to
	 * provide a unique value for each consumer.
	 * @param clientId the client id.
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * Set the user defined {@link ConsumerRebalanceListener} implementation.
	 * @param consumerRebalanceListener the {@link ConsumerRebalanceListener} instance
	 */
	public void setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
		this.consumerRebalanceListener = consumerRebalanceListener;
	}

	public ConsumerRebalanceListener getConsumerRebalanceListener() {
		return this.consumerRebalanceListener;
	}

	/**
	 * Set the timeout for commitSync operations (if {@link #isSyncCommits()}. Overrides
	 * the default api timeout property.
	 * @param syncCommitTimeout the timeout.
	 * @see #setSyncCommits(boolean)
	 */
	public void setSyncCommitTimeout(@Nullable Duration syncCommitTimeout) {
		this.syncCommitTimeout = syncCommitTimeout;
	}

	public Duration getSyncCommitTimeout() {
		return this.syncCommitTimeout;
	}

	/**
	 * Set the commit callback; by default a simple logging callback is used to log
	 * success at DEBUG level and failures at ERROR level.
	 * Used when {@link #setSyncCommits(boolean) syncCommits} is false.
	 * @param commitCallback the callback.
	 * @see #setSyncCommits(boolean)
	 */
	public void setCommitCallback(OffsetCommitCallback commitCallback) {
		this.commitCallback = commitCallback;
	}

	public OffsetCommitCallback getCommitCallback() {
		return this.commitCallback;
	}

	/**
	 * Set whether or not to call consumer.commitSync() or commitAsync() when the
	 * container is responsible for commits. Default true.
	 * @param syncCommits true to use commitSync().
	 * @see #setSyncCommitTimeout(Duration)
	 * @see #setCommitCallback(OffsetCommitCallback)
	 * @see #setCommitLogLevel(org.springframework.kafka.support.LogIfLevelEnabled.Level)
	 */
	public void setSyncCommits(boolean syncCommits) {
		this.syncCommits = syncCommits;
	}

	public boolean isSyncCommits() {
		return this.syncCommits;
	}

	/**
	 * The level at which to log offset commits.
	 * @return the level.
	 */
	public LogIfLevelEnabled.Level getCommitLogLevel() {
		return this.commitLogLevel;
	}

	/**
	 * Set the level at which to log offset commits.
	 * Default: DEBUG.
	 * @param commitLogLevel the level.
	 */
	public void setCommitLogLevel(LogIfLevelEnabled.Level commitLogLevel) {
		Assert.notNull(commitLogLevel, "'commitLogLevel' cannot be null");
		this.commitLogLevel = commitLogLevel;
	}

	/**
	 * Get the consumer properties that will be merged with the consumer properties
	 * provided by the consumer factory; properties here will supersede any with the same
	 * name(s) in the consumer factory.
	 * {@code group.id} and {@code client.id} are ignored.
	 * @return the properties.
	 * @see org.apache.kafka.clients.consumer.ConsumerConfig
	 * @see #setGroupId(String)
	 * @see #setClientId(String)
	 */
	public Properties getKafkaConsumerProperties() {
		return this.kafkaConsumerProperties;
	}

	/**
	 * Set the consumer properties that will be merged with the consumer properties
	 * provided by the consumer factory; properties here will supersede any with the same
	 * name(s) in the consumer factory.
	 * {@code group.id} and {@code client.id} are ignored.
	 * @param kafkaConsumerProperties the properties.
	 * @see org.apache.kafka.clients.consumer.ConsumerConfig
	 * @see #setGroupId(String)
	 * @see #setClientId(String)
	 */
	public void setKafkaConsumerProperties(Properties kafkaConsumerProperties) {
		Assert.notNull(kafkaConsumerProperties, "'kafkaConsumerProperties' cannot be null");
		this.kafkaConsumerProperties = kafkaConsumerProperties;
	}

	@Override
	public String toString() {
		return "ConsumerProperties ["
				+ renderProperties()
				+ "]";
	}

	protected final String renderProperties() {
		return renderTopics()
				+ ", pollTimeout=" + this.pollTimeout
				+ (this.groupId != null ? ", groupId=" + this.groupId : "")
				+ (StringUtils.hasText(this.clientId) ? ", clientId=" + this.clientId : "")
				+ (this.consumerRebalanceListener != null
						? ", consumerRebalanceListener=" + this.consumerRebalanceListener
						: "")
				+ (this.commitCallback != null ? ", commitCallback=" + this.commitCallback : "")
				+ ", syncCommits=" + this.syncCommits
				+ (this.syncCommitTimeout != null ? ", syncCommitTimeout=" + this.syncCommitTimeout : "")
				+ (this.kafkaConsumerProperties.size() > 0 ? ", properties=" + this.kafkaConsumerProperties : "");
	}

	private String renderTopics() {
		return (this.topics != null ? "topics=" + Arrays.toString(this.topics) : "")
				+ (this.topicPattern != null ? ", topicPattern=" + this.topicPattern : "")
				+ (this.topicPartitionsToAssign != null
						? ", topicPartitions=" + Arrays.toString(this.topicPartitionsToAssign)
						: "");
	}

}
