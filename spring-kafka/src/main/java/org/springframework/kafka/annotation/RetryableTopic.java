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

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConstants;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;

/**
 *
 * Annotation to create the retry and dlt topics for a {@link KafkaListener} annotated
 * listener. See {@link org.springframework.kafka.retrytopic.RetryTopicConfigurer} for
 * usage examples. All String properties can be resolved from property placeholders
 * {@code ${...}} or SpEL expressions {@code #{...}}.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 * @see org.springframework.kafka.retrytopic.RetryTopicConfigurer
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RetryableTopic {

	/**
	 * The number of attempts made before the message is sent to the DLT. Expressions must
	 * resolve to an integer or a string that can be parsed as such. Default 3.
	 * @return the number of attempts.
	 */
	String attempts() default "3";

	/**
	 * Specify the backoff properties for retrying this operation. The default is a simple
	 * {@link Backoff} specification with no properties - see it's documentation for
	 * defaults.
	 * @return a backoff specification
	 */
	Backoff backoff() default @Backoff;

	/**
	 *
	 * The amount of time in milliseconds after which message retrying should give up and
	 * send the message to the DLT. Expressions must resolv to a long or a String that can
	 * be parsed as such.
	 * @return the timeout value.
	 *
	 */
	String timeout() default "";

	/**
	 *
	 * The bean name of the {@link org.springframework.kafka.core.KafkaTemplate} bean that
	 * will be used to forward the message to the retry and Dlt topics. If not specified,
	 * a bean with name {@code retryTopicDefaultKafkaTemplate} or {@code kafkaTemplate}
	 * will be looked up.
	 *
	 * @return the kafkaTemplate bean name.
	 */
	String kafkaTemplate() default "";

	/**
	 * The bean name of the
	 * {@link org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory}
	 * that will be used to create the consumers for the retry and dlt topics. If none is
	 * provided, the one from the {@link KafkaListener} annotation is used, or else a
	 * default one, if any.
	 *
	 * @return the listenerContainerFactory bean name.
	 */
	String listenerContainerFactory() default "";

	/**
	 * Whether or not the topics should be created after registration with the provided
	 * configurations. Not to be confused with the
	 * ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG from Kafka configuration, which is
	 * handled by the {@link org.apache.kafka.clients.consumer.KafkaConsumer}.
	 * Expressions must resolve to a boolean or a String that can be parsed as such.
	 * @return the configuration.
	 */
	String autoCreateTopics() default "true";

	/**
	 * The number of partitions for the automatically created topics. Expressions must
	 * resolve to an integer or a String that can be parsed as such. Default 1.
	 * @return the number of partitions.
	 */
	String numPartitions() default "1";

	/**
	 * The replication factor for the automatically created topics. Expressions must
	 * resolve to a short or a String that can be parsed as such.
	 * @return the replication factor.
	 */
	String replicationFactor() default "1";

	/**
	 * The exception types that should be retried.
	 * @return the exceptions.
	 */
	Class<? extends Throwable>[] include() default {};

	/**
	 * The exception types that should not be retried. When the message processing throws
	 * these exceptions the message goes straight to the DLT.
	 * @return the exceptions not to be retried.
	 */
	Class<? extends Throwable>[] exclude() default {};

	/**
	 * The exception class names that should be retried.
	 * @return the exceptions.
	 */
	String[] includeNames() default {};

	/**
	 * The exception class names that should not be retried. When the message processing
	 * throws these exceptions the message goes straight to the DLT.
	 * @return the exceptions not to be retried.
	 */
	String[] excludeNames() default {};

	/**
	 * Whether or not the captured exception should be traversed to look for the
	 * exceptions provided above. Expressions must resolve to a boolean or a String that
	 * can be parsed as such. Default true when {@link #include()} or {@link #exclude()}
	 * provided; false otherwise.
	 * @return the value.
	 */
	String traversingCauses() default "";

	/**
	 * The suffix that will be appended to the main topic in order to generate the retry
	 * topics. The corresponding delay value is also appended.
	 * @return the retry topics' suffix.
	 */
	String retryTopicSuffix() default RetryTopicConstants.DEFAULT_RETRY_SUFFIX;

	/**
	 * The suffix that will be appended to the main topic in order to generate the dlt
	 * topic.
	 * @return the dlt suffix.
	 */
	String dltTopicSuffix() default RetryTopicConstants.DEFAULT_DLT_SUFFIX;

	/**
	 * Whether the retry topics will be suffixed with the delay value for that topic or a
	 * simple index.
	 * @return the strategy.
	 */
	TopicSuffixingStrategy topicSuffixingStrategy() default TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE;

	/**
	 * Whether or not to redeliver to the DLT if delivery fails.
	 * @return the dlt strategy.
	 */
	DltStrategy dltStrategy() default DltStrategy.ALWAYS_RETRY_ON_ERROR;

	/**
	 * Whether to use a single or multiple topics when using a fixed delay.
	 * @return the fixed delay strategy.
	 */
	FixedDelayStrategy fixedDelayTopicStrategy() default FixedDelayStrategy.MULTIPLE_TOPICS;

}
