/**
 * <h2>Retry Topic Infrastructure</h2>
 *
 * <p>This package provides comprehensive support for implementing retry patterns with Kafka,
 * featuring automatic retry topic management and dead-letter topic (DLT) handling.
 *
 * <h3>Core Concepts</h3>
 *
 * <p><b>Retry Topics:</b> When message processing fails, messages are automatically sent to
 * retry topics with configurable back-off delays, allowing for progressive retry attempts
 * without blocking the main consumer thread.
 *
 * <p><b>Dead-Letter Topics (DLT):</b> Messages that exhaust all retry attempts are routed to
 * a dead-letter topic for manual inspection, reprocessing, or logging.
 *
 * <h3>Key Components</h3>
 *
 * <ul>
 *   <li>{@link org.springframework.kafka.retrytopic.RetryTopicConfiguration RetryTopicConfiguration} -
 *       Main configuration container for retry behavior</li>
 *   <li>{@link org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder RetryTopicConfigurationBuilder} -
 *       Fluent API for building retry configurations</li>
 *   <li>{@link org.springframework.kafka.retrytopic.DestinationTopic DestinationTopic} -
 *       Represents a single retry or DLT destination</li>
 *   <li>{@link org.springframework.kafka.retrytopic.DestinationTopicResolver DestinationTopicResolver} -
 *       Resolves which destination topic to use for failed messages</li>
 *   <li>{@link org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory DeadLetterPublishingRecovererFactory} -
 *       Creates recoverers that publish failed messages to appropriate destinations</li>
 *   <li>{@link org.springframework.kafka.retrytopic.DltStrategy DltStrategy} -
 *       Defines strategies for DLT handling (always send, only on certain exceptions, etc.)</li>
 * </ul>
 *
 * <h3>Typical Usage</h3>
 *
 * <p>Configure retry topics using the {@code @RetryableTopic} annotation on listener methods,
 * or programmatically via {@link org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder}.
 * The framework automatically creates the necessary retry topic infrastructure and routes
 * failed messages through the configured retry chain.
 *
 * <h3>Back-off Configuration</h3>
 *
 * <p>The package supports various back-off strategies:
 * <ul>
 *   <li>Fixed delay between retries</li>
 *   <li>Exponential back-off with configurable multiplier</li>
 *   <li>Maximum retry attempts</li>
 *   <li>Custom back-off policies</li>
 * </ul>
 *
 * @since 2.7
 */
@org.jspecify.annotations.NullMarked
package org.springframework.kafka.retrytopic;
