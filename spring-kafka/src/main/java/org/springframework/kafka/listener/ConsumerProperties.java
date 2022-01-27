/*
 * Copyright 2019-2022 the original author or authors.
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

	private static final int DEFAULT_COMMIT_RETRIES = 3;

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
	private final TopicPartitionOffset[] topicPartitions;

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

	private boolean onlyLogRecordMetadata = true;

	private Properties kafkaConsumerProperties = new Properties();

	private Duration authExceptionRetryInterval;

	private int commitRetries = DEFAULT_COMMIT_RETRIES;

	private boolean fixTxOffsets;

	private boolean checkDeserExWhenKeyNull;

	private boolean checkDeserExWhenValueNull;

	/**
	 * Create properties for a container that will subscribe to the specified topics.
	 * @param topics the topics.
	 */
	public ConsumerProperties(String... topics) {
		Assert.notEmpty(topics, "An array of topics must be provided");
		this.topics = topics.clone();
		this.topicPattern = null;
		this.topicPartitions = null;
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
		this.topicPartitions = null;
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
		this.topicPartitions = Arrays.copyOf(topicPartitions, topicPartitions.length);
	}

	/**
	 * Return the configured topics.
	 * @return the topics.
	 */
	@Nullable
	public String[] getTopics() {
		return this.topics != null
				? Arrays.copyOf(this.topics, this.topics.length)
				: null;
	}

	/**
	 * Return the configured topic pattern.
	 * @return the topic pattern.
	 */
	@Nullable
	public Pattern getTopicPattern() {
		return this.topicPattern;
	}

	/**
	 * Return the configured {@link TopicPartitionOffset}s.
	 * @return the topics/partitions.
	 * @since 2.5
	 */
	@Nullable
	public TopicPartitionOffset[] getTopicPartitions() {
		return this.topicPartitions != null
				? Arrays.copyOf(this.topicPartitions, this.topicPartitions.length)
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

	/**
	 * Return the container's group id.
	 * @return the group id.
	 */
	@Nullable
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

	/**
	 * Return the rebalance listener.
	 * @return the listener.
	 */
	@Nullable
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

	/**
	 * Return the sync commit timeout.
	 * @return the timeout.
	 */
	@Nullable
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

	/**
	 * Return the commit callback.
	 * @return the callback.
	 */
	@Nullable
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
	 * @see #setCommitRetries(int)
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
	 * name(s) in the consumer factory. You can add non-String-valued properties, but the
	 * property name (hashtable key) must be String; all others will be ignored.
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
	 * Property keys must be {@link String}s.
	 * @param kafkaConsumerProperties the properties.
	 * @see org.apache.kafka.clients.consumer.ConsumerConfig
	 * @see #setGroupId(String)
	 * @see #setClientId(String)
	 */
	public void setKafkaConsumerProperties(Properties kafkaConsumerProperties) {
		Assert.notNull(kafkaConsumerProperties, "'kafkaConsumerProperties' cannot be null");
		this.kafkaConsumerProperties = kafkaConsumerProperties;
	}

	/**
	 * Get the authentication/authorization retry interval.
	 * @return the interval.
	 * @deprecated in favor of {@link #getAuthExceptionRetryInterval()}.
	 */
	@Deprecated
	@Nullable
	public Duration getAuthorizationExceptionRetryInterval() {
		return this.authExceptionRetryInterval;
	}

	/**
	 * Set the interval between retries after and
	 * {@link org.apache.kafka.common.errors.AuthenticationException} or
	 * {@code org.apache.kafka.common.errors.AuthorizationException} is thrown by
	 * {@code KafkaConsumer}. By default the field is null and retries are disabled. In
	 * such case the container will be stopped.
	 *
	 * The interval must be less than {@code max.poll.interval.ms} consumer property.
	 *
	 * @param authorizationExceptionRetryInterval the duration between retries
	 * @since 2.3.5
	 * @deprecated in favor of {@link #setAuthExceptionRetryInterval(Duration)}.
	 */
	@Deprecated
	public void setAuthorizationExceptionRetryInterval(Duration authorizationExceptionRetryInterval) {
		this.authExceptionRetryInterval = authorizationExceptionRetryInterval;
	}

	/**
	 * Get the authentication/authorization retry interval.
	 * @return the interval.
	 */
	@Nullable
	public Duration getAuthExceptionRetryInterval() {
		return this.authExceptionRetryInterval;
	}

	/**
	 * Set the interval between retries after and
	 * {@link org.apache.kafka.common.errors.AuthenticationException} or
	 * {@code org.apache.kafka.common.errors.AuthorizationException} is thrown by
	 * {@code KafkaConsumer}. By default the field is null and retries are disabled. In
	 * such case the container will be stopped.
	 *
	 * The interval must be less than {@code max.poll.interval.ms} consumer property.
	 *
	 * @param authExceptionRetryInterval the duration between retries
	 * @since 2.8
	 */
	public void setAuthExceptionRetryInterval(Duration authExceptionRetryInterval) {
		this.authExceptionRetryInterval = authExceptionRetryInterval;
	}

	/**
	 * The number of retries allowed when a
	 * {@link org.apache.kafka.clients.consumer.RetriableCommitFailedException} is thrown
	 * by the consumer when using {@link #setSyncCommits(boolean)} set to true.
	 * @return the number of retries.
	 * @since 2.3.9
	 * @see #setSyncCommits(boolean)
	 */
	public int getCommitRetries() {
		return this.commitRetries;
	}

	/**
	 * Set number of retries allowed when a
	 * {@link org.apache.kafka.clients.consumer.RetriableCommitFailedException} is thrown
	 * by the consumer when using {@link #setSyncCommits(boolean)} set to true. Default 3
	 * (4 attempts total).
	 * @param commitRetries the commitRetries.
	 * @since 2.3.9
	 * @see #setSyncCommits(boolean)
	 */
	public void setCommitRetries(int commitRetries) {
		this.commitRetries = commitRetries;
	}

	public boolean isOnlyLogRecordMetadata() {
		return this.onlyLogRecordMetadata;
	}

	/**
	 * Set to false to log {@code record.toString()} in log messages instead
	 * of {@code topic-partition@offset}.
	 * @param onlyLogRecordMetadata false to log the entire record.
	 * @since 2.2.14
	 */
	public void setOnlyLogRecordMetadata(boolean onlyLogRecordMetadata) {
		this.onlyLogRecordMetadata = onlyLogRecordMetadata;
	}

	/**
	 * Whether or not to correct terminal transactional offsets.
	 * @return true to fix.
	 * @since 2.5.6
	 * @see #setFixTxOffsets(boolean)
	 */
	public boolean isFixTxOffsets() {
		return this.fixTxOffsets;
	}

	/**
	 * When consuming records produced by a transactional producer, and the consumer is
	 * positioned at the end of a partition, the lag can incorrectly be reported as
	 * greater than zero, due to the pseudo record used to indicate transaction
	 * commit/rollback and, possibly, the presence of rolled-back records. This does not
	 * functionally affect the consumer but some users have expressed concern that the
	 * "lag" is non-zero. Set this to true and the container will correct such
	 * mis-reported offsets. The check is performed before the next poll to avoid adding
	 * significant complexity to the commit processing. IMPORTANT: At the time of writing,
	 * the lag will only be corrected if the consumer is configured with
	 * {@code isolation.level=read_committed} and {@code max.poll.records} is greater than
	 * 1. See https://issues.apache.org/jira/browse/KAFKA-10683 for more information.
	 * @param fixTxOffsets true to correct the offset(s).
	 * @since 2.5.6
	 */
	public void setFixTxOffsets(boolean fixTxOffsets) {
		this.fixTxOffsets = fixTxOffsets;
	}

	/**
	 * Always check for a deserialization exception header with a null key.
	 * @return true to check.
	 * @since 2.8.1
	 */
	public boolean isCheckDeserExWhenKeyNull() {
		return this.checkDeserExWhenKeyNull;
	}

	/**
	 * Set to true to always check for
	 * {@link org.springframework.kafka.support.serializer.DeserializationException}
	 * header when a null key is received. Useful when the consumer code cannot determine
	 * that an
	 * {@link org.springframework.kafka.support.serializer.ErrorHandlingDeserializer} has
	 * been configured, such as when using a delegating deserializer.
	 * @param checkDeserExWhenKeyNull true to always check.
	 * @since 2.8.1
	 */
	public void setCheckDeserExWhenKeyNull(boolean checkDeserExWhenKeyNull) {
		this.checkDeserExWhenKeyNull = checkDeserExWhenKeyNull;
	}

	/**
	 * Always check for a deserialization exception header with a null value.
	 * @return true to check.
	 * @since 2.8.1
	 */
	public boolean isCheckDeserExWhenValueNull() {
		return this.checkDeserExWhenValueNull;
	}

	/**
	 * Set to true to always check for
	 * {@link org.springframework.kafka.support.serializer.DeserializationException}
	 * header when a null value is received. Useful when the consumer code cannot
	 * determine that an
	 * {@link org.springframework.kafka.support.serializer.ErrorHandlingDeserializer} has
	 * been configured, such as when using a delegating deserializer.
	 * @param checkDeserExWhenValueNull true to always check.
	 * @since 2.8.1
	 */
	public void setCheckDeserExWhenValueNull(boolean checkDeserExWhenValueNull) {
		this.checkDeserExWhenValueNull = checkDeserExWhenValueNull;
	}

	@Override
	public String toString() {
		return "ConsumerProperties ["
				+ renderProperties()
				+ "]";
	}

	protected final String renderProperties() {
		return renderTopics()
				+ "\n pollTimeout=" + this.pollTimeout
				+ (this.groupId != null ? "\n groupId=" + this.groupId : "")
				+ (StringUtils.hasText(this.clientId) ? "\n clientId=" + this.clientId : "")
				+ (this.consumerRebalanceListener != null
						? "\n consumerRebalanceListener=" + this.consumerRebalanceListener
						: "")
				+ (this.commitCallback != null ? "\n commitCallback=" + this.commitCallback : "")
				+ "\n syncCommits=" + this.syncCommits
				+ (this.syncCommitTimeout != null ? "\n syncCommitTimeout=" + this.syncCommitTimeout : "")
				+ (this.kafkaConsumerProperties.size() > 0 ? "\n properties=" + this.kafkaConsumerProperties : "")
				+ "\n authExceptionRetryInterval=" + this.authExceptionRetryInterval
				+ "\n commitRetries=" + this.commitRetries
				+ "\n fixTxOffsets" + this.fixTxOffsets;
	}

	private String renderTopics() {
		return (this.topics != null ? "\n topics=" + Arrays.toString(this.topics) : "")
				+ (this.topicPattern != null ? "\n topicPattern=" + this.topicPattern : "")
				+ (this.topicPartitions != null
						? "\n topicPartitions=" + Arrays.toString(this.topicPartitions)
						: "");
	}

}
