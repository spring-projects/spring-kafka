/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be the target of a Kafka message listener on the
 * specified topics.
 *
 * The {@link #containerFactory()} identifies the
 * {@link org.springframework.kafka.config.KafkaListenerContainerFactory
 * KafkaListenerContainerFactory} to use to build the Kafka listener container. If not
 * set, a <em>default</em> container factory is assumed to be available with a bean name
 * of {@code kafkaListenerContainerFactory} unless an explicit default has been provided
 * through configuration.
 *
 * <p>
 * Processing of {@code @KafkaListener} annotations is performed by registering a
 * {@link KafkaListenerAnnotationBeanPostProcessor}. This can be done manually or, more
 * conveniently, through {@link EnableKafka} annotation.
 *
 * <p>
 * Annotated methods are allowed to have flexible signatures similar to what
 * {@link MessageMapping} provides, that is
 * <ul>
 * <li>{@link org.apache.kafka.clients.consumer.ConsumerRecord} to access to the raw Kafka
 * message</li>
 * <li>{@link org.springframework.kafka.support.Acknowledgment} to manually ack</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Payload @Payload}-annotated
 * method arguments including the support of validation</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Header @Header}-annotated
 * method arguments to extract a specific header value, defined by
 * {@link org.springframework.kafka.support.KafkaHeaders KafkaHeaders}</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Headers @Headers}-annotated
 * argument that must also be assignable to {@link java.util.Map} for getting access to
 * all headers.</li>
 * <li>{@link org.springframework.messaging.MessageHeaders MessageHeaders} arguments for
 * getting access to all headers.</li>
 * <li>{@link org.springframework.messaging.support.MessageHeaderAccessor
 * MessageHeaderAccessor} for convenient access to all method arguments.</li>
 * </ul>
 *
 * <p>When defined at the method level, a listener container is created for each method.
 * The {@link org.springframework.kafka.listener.MessageListener} is a
 * {@link org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter},
 * configured with a {@link org.springframework.kafka.config.MethodKafkaListenerEndpoint}.
 *
 * <p>When defined at the class level, a single message listener container is used to
 * service all methods annotated with {@code @KafkaHandler}. Method signatures of such
 * annotated methods must not cause any ambiguity such that a single method can be
 * resolved for a particular inbound message. The
 * {@link org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter} is
 * configured with a
 * {@link org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint}.
 *
 * @author Gary Russell
 * @author Venil Noronha
 *
 * @see EnableKafka
 * @see KafkaListenerAnnotationBeanPostProcessor
 * @see KafkaListeners
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Repeatable(KafkaListeners.class)
public @interface KafkaListener {

	/**
	 * The unique identifier of the container managing for this endpoint.
	 * <p>If none is specified an auto-generated one is provided.
	 * <p>Note: When provided, this value will override the group id property
	 * in the consumer factory configuration, unless {@link #idIsGroup()}
	 * is set to false.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return the {@code id} for the container managing for this endpoint.
	 * @see org.springframework.kafka.config.KafkaListenerEndpointRegistry#getListenerContainer(String)
	 */
	String id() default "";

	/**
	 * The bean name of the {@link org.springframework.kafka.config.KafkaListenerContainerFactory}
	 * to use to create the message listener container responsible to serve this endpoint.
	 * <p>If not specified, the default container factory is used, if any.
	 * @return the container factory bean name.
	 */
	String containerFactory() default "";

	/**
	 * The topics for this listener.
	 * The entries can be 'topic name', 'property-placeholder keys' or 'expressions'.
	 * An expression must be resolved to the topic name.
	 * Mutually exclusive with {@link #topicPattern()} and {@link #topicPartitions()}.
	 * @return the topic names or expressions (SpEL) to listen to.
	 */
	String[] topics() default {};

	/**
	 * The topic pattern for this listener.
	 * The entries can be 'topic name', 'property-placeholder keys' or 'expressions'.
	 * An expression must be resolved to the topic pattern.
	 * Mutually exclusive with {@link #topics()} and {@link #topicPartitions()}.
	 * @return the topic pattern or expression (SpEL).
	 */
	String topicPattern() default "";

	/**
	 * The topicPartitions for this listener.
	 * Mutually exclusive with {@link #topicPattern()} and {@link #topics()}.
	 * @return the topic names or expressions (SpEL) to listen to.
	 */
	TopicPartition[] topicPartitions() default {};

	/**
	 * If provided, the listener container for this listener will be added to a bean
	 * with this value as its name, of type {@code Collection<MessageListenerContainer>}.
	 * This allows, for example, iteration over the collection to start/stop a subset
	 * of containers.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return the bean name for the group.
	 */
	String containerGroup() default "";

	/**
	 * Set an {@link org.springframework.kafka.listener.KafkaListenerErrorHandler} bean
	 * name to invoke if the listener method throws an exception.
	 * @return the error handler.
	 * @since 1.3
	 */
	String errorHandler() default "";

	/**
	 * Override the {@code group.id} property for the consumer factory with this value
	 * for this listener only.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return the group id.
	 * @since 1.3
	 */
	String groupId() default "";

	/**
	 * When {@link #groupId() groupId} is not provided, use the {@link #id() id} (if
	 * provided) as the {@code group.id} property for the consumer. Set to false, to use
	 * the {@code group.id} from the consumer factory.
	 * @return false to disable.
	 * @since 1.3
	 */
	boolean idIsGroup() default true;

	/**
	 * When provided, overrides the client id property in the consumer factory
	 * configuration. A suffix ('-n') is added for each container instance to ensure
	 * uniqueness when concurrency is used.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return the client id prefix.
	 * @since 2.1.1
	 */
	String clientIdPrefix() default "";

	/**
	 * A pseudo bean name used in SpEL expressions within this annotation to reference
	 * the current bean within which this listener is defined. This allows access to
	 * properties and methods within the enclosing bean.
	 * Default '__listener'.
	 * <p>
	 * Example: {@code topics = "#{__listener.topicList}"}.
	 * @return the pseudo bean name.
	 * @since 2.1.2
	 */
	String beanRef() default "__listener";

	/**
	 * Override the container factory's {@code concurrency} setting for this listener. May
	 * be a property placeholder or SpEL expression that evaluates to a {@link Number}, in
	 * which case {@link Number#intValue()} is used to obtain the value.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return the concurrency.
	 * @since 2.2
	 */
	String concurrency() default "";

	/**
	 * Set to true or false, to override the default setting in the container factory. May
	 * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
	 * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
	 * obtain the value.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return true to auto start, false to not auto start.
	 * @since 2.2
	 */
	String autoStartup() default "";

}
