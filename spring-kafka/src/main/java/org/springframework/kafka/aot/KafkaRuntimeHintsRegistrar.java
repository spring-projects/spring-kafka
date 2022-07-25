/*
 * Copyright 2022 the original author or authors.
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
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.clients.producer.UniformStickyPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.ListDeserializer;
import org.apache.kafka.common.serialization.ListSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser.AppInfo;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.support.RuntimeHintsUtils;
import org.springframework.kafka.annotation.KafkaBootstrapConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
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
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.serializer.DelegatingByTopicDeserializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.DelegatingDeserializer;
import org.springframework.kafka.support.serializer.DelegatingSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.ParseStringDeserializer;
import org.springframework.kafka.support.serializer.StringOrBytesSerializer;
import org.springframework.kafka.support.serializer.ToStringSerializer;
import org.springframework.lang.Nullable;

/**
 * {@link RuntimeHintsRegistrar} for Spring for Apache Kafka.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class KafkaRuntimeHintsRegistrar implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		RuntimeHintsUtils.registerAnnotation(hints, KafkaListener.class);
		RuntimeHintsUtils.registerAnnotation(hints, KafkaListeners.class);
		RuntimeHintsUtils.registerAnnotation(hints, PartitionOffset.class);
		RuntimeHintsUtils.registerAnnotation(hints, TopicPartition.class);
		ReflectionHints reflectionHints = hints.reflection();
		Stream.of(
					ConsumerProperties.class,
					ContainerProperties.class,
					ProducerListener.class)
				.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_METHODS)));

		Stream.of(
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
					KafkaOperations.class,
					ConsumerFactory.class,
					LoggingProducerListener.class,
					ImplicitLinkedHashCollection.Element.class,
					KafkaListenerAnnotationBeanPostProcessor.class)
				.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
								MemberCategory.INVOKE_DECLARED_METHODS,
								MemberCategory.INTROSPECT_PUBLIC_METHODS)));

		Stream.of(
					KafkaBootstrapConfiguration.class,
					CreatableTopic.class,
					KafkaListenerEndpointRegistry.class)
				.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS)));

		Stream.of(
					AppInfo.class,
					// standard assignors
					CooperativeStickyAssignor.class,
					RangeAssignor.class,
					RoundRobinAssignor.class,
					StickyAssignor.class,
					// standard partitioners
					DefaultPartitioner.class,
					RoundRobinPartitioner.class,
					UniformStickyPartitioner.class,
					// standard serialization
					ByteArrayDeserializer.class,
					ByteArraySerializer.class,
					ByteBufferDeserializer.class,
					ByteBufferSerializer.class,
					BytesDeserializer.class,
					BytesSerializer.class,
					DoubleSerializer.class,
					DoubleDeserializer.class,
					FloatSerializer.class,
					FloatDeserializer.class,
					IntegerSerializer.class,
					IntegerDeserializer.class,
					ListDeserializer.class,
					ListSerializer.class,
					LongSerializer.class,
					LongDeserializer.class,
					StringDeserializer.class,
					StringSerializer.class,
					// Spring serialization
					DelegatingByTopicDeserializer.class,
					DelegatingByTypeSerializer.class,
					DelegatingDeserializer.class,
					ErrorHandlingDeserializer.class,
					DelegatingSerializer.class,
					JsonDeserializer.class,
					JsonSerializer.class,
					ParseStringDeserializer.class,
					StringOrBytesSerializer.class,
					ToStringSerializer.class,
					Serdes.class,
					Serdes.ByteArraySerde.class,
					Serdes.BytesSerde.class,
					Serdes.ByteBufferSerde.class,
					Serdes.DoubleSerde.class,
					Serdes.FloatSerde.class,
					Serdes.IntegerSerde.class,
					Serdes.LongSerde.class,
					Serdes.ShortSerde.class,
					Serdes.StringSerde.class,
					Serdes.UUIDSerde.class,
					Serdes.VoidSerde.class,
					CRC32C.class)
				.forEach(type -> reflectionHints.registerType(type, builder ->
						builder.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS)));

		hints.proxies().registerJdkProxy(AopProxyUtils.completeJdkProxyInterfaces(Consumer.class));
		hints.proxies().registerJdkProxy(AopProxyUtils.completeJdkProxyInterfaces(Producer.class));

		Stream.of(
				"org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor",
				"org.apache.kafka.streams.errors.DefaultProductionExceptionHandler",
				"org.apache.kafka.streams.processor.FailOnInvalidTimestamp",
				"org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor",
				"org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor",
				"org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor",
				"org.apache.kafka.streams.errors.LogAndFailExceptionHandler")
			.forEach(type -> reflectionHints.registerTypeIfPresent(classLoader, type, builder ->
					builder.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS)));
	}

}
