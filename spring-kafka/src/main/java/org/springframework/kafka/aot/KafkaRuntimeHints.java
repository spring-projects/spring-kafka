/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.kafka.aot;

import java.util.stream.Stream;
import java.util.zip.CRC32C;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.AppInfoParser.AppInfo;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.jspecify.annotations.Nullable;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.kafka.annotation.KafkaBootstrapConfiguration;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaResourceFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.KafkaMessageHeaderAccessor;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.serializer.DelegatingByTopicDeserializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.DelegatingDeserializer;
import org.springframework.kafka.support.serializer.DelegatingSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.support.serializer.ParseStringDeserializer;
import org.springframework.kafka.support.serializer.StringOrBytesSerializer;
import org.springframework.kafka.support.serializer.ToStringSerializer;

/**
 * {@link RuntimeHintsRegistrar} for Spring for Apache Kafka.
 *
 * @author Gary Russell
 * @author Soby Chacko
 * @since 3.0
 *
 */
public class KafkaRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	@SuppressWarnings("removal")
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		ReflectionHints reflectionHints = hints.reflection();
		Stream.of(
						ConsumerProperties.class,
						ContainerProperties.class,
						KafkaMessageHeaderAccessor.class,
						ProducerListener.class)
				.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_METHODS)));

		Stream.of(
						// Following Kafka classes need to be ideally part of Oracle's reachability metadata repository.
						DefaultJwtRetriever.class,
						DefaultJwtValidator.class,
						Message.class,
						ImplicitLinkedHashCollection.Element.class,
						NewTopic.class,
						AbstractKafkaListenerContainerFactory.class,
						ConcurrentKafkaListenerContainerFactory.class,
						KafkaListenerContainerFactory.class,
						KafkaListenerEndpointRegistry.class,
						DefaultKafkaConsumerFactory.class,
						DefaultKafkaProducerFactory.class,
						KafkaAdmin.class,
						KafkaOperations.class,
						KafkaResourceFactory.class,
						KafkaTemplate.class,
						ProducerFactory.class,
						ConsumerFactory.class,
						LoggingProducerListener.class,
						KafkaListenerAnnotationBeanPostProcessor.class)
				.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
								MemberCategory.INVOKE_DECLARED_METHODS)));

		Stream.of(
						KafkaBootstrapConfiguration.class,
						CreatableTopic.class)
				.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS)));

		Stream.of(
						AppInfo.class,
						// standard serialization
						// Spring serialization
						DelegatingByTopicDeserializer.class,
						DelegatingByTypeSerializer.class,
						DelegatingDeserializer.class,
						ErrorHandlingDeserializer.class,
						DelegatingSerializer.class,
						org.springframework.kafka.support.serializer.JsonDeserializer.class,
						org.springframework.kafka.support.serializer.JsonSerializer.class,
						JacksonJsonDeserializer.class,
						JacksonJsonSerializer.class,
						ParseStringDeserializer.class,
						StringOrBytesSerializer.class,
						ToStringSerializer.class,
						Serdes.class,
						CRC32C.class)
				.forEach(type -> reflectionHints.registerType(type, builder ->
						builder.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS)));

		hints.proxies().registerJdkProxy(AopProxyUtils.completeJdkProxyInterfaces(Consumer.class));
		hints.proxies().registerJdkProxy(AopProxyUtils.completeJdkProxyInterfaces(Producer.class));

		Stream.of(
						"sun.security.provider.ConfigFile",
						"org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor",
						"org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor",
						"org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler")
				.forEach(type -> reflectionHints.registerTypeIfPresent(classLoader, type, builder ->
						builder.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS)));
	}

}
