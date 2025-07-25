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
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.ParseStringDeserializer;
import org.springframework.kafka.support.serializer.StringOrBytesSerializer;
import org.springframework.kafka.support.serializer.ToStringSerializer;
import org.springframework.kafka.support.serializer.JsonTypeResolver;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;

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
					JsonDeserializer.class,
					JsonSerializer.class,
					ParseStringDeserializer.class,
					StringOrBytesSerializer.class,
					ToStringSerializer.class,
					Serdes.class,
					CRC32C.class)
				.forEach(type -> reflectionHints.registerType(type, builder ->
						builder.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS)));

		registerJsonDeserializerDynamicHints(reflectionHints, classLoader);

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

	/**
	 * Register enhanced runtime hints for JsonDeserializer dynamic class loading and reflection.
	 * This method addresses GraalVM native image issues with:
	 * - ClassUtils.forName() calls in buildTypeResolver()
	 * - Dynamic method invocation for type resolution
	 * - JsonTypeResolver implementations
	 * 
	 * @param reflectionHints the reflection hints to register
	 * @param classLoader the class loader to use for type resolution
	 */
	private void registerJsonDeserializerDynamicHints(ReflectionHints reflectionHints, @Nullable ClassLoader classLoader) {
		// JsonTypeResolver interface and implementations need full reflection access
		reflectionHints.registerType(JsonTypeResolver.class, builder ->
				builder.withMembers(MemberCategory.INVOKE_DECLARED_METHODS,
							MemberCategory.INVOKE_PUBLIC_METHODS));

		// DefaultJackson2JavaTypeMapper for dynamic type mapping
		reflectionHints.registerType(DefaultJackson2JavaTypeMapper.class, builder ->
				builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_DECLARED_METHODS));

		// Jackson JavaType for dynamic type resolution
		reflectionHints.registerTypeIfPresent(classLoader, "com.fasterxml.jackson.databind.JavaType", builder ->
				builder.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
							MemberCategory.INVOKE_PUBLIC_METHODS,
							MemberCategory.INVOKE_DECLARED_METHODS));

		// TypeFactory for dynamic type construction in buildTypeResolver
		reflectionHints.registerTypeIfPresent(classLoader, "com.fasterxml.jackson.databind.type.TypeFactory", builder ->
				builder.withMembers(MemberCategory.INVOKE_PUBLIC_METHODS,
							MemberCategory.INVOKE_DECLARED_METHODS));

		// Method class for reflection-based invocation in buildTypeResolver
		reflectionHints.registerType(java.lang.reflect.Method.class, builder ->
				builder.withMembers(MemberCategory.INVOKE_PUBLIC_METHODS));

		// Class.forName and related reflection utilities
		reflectionHints.registerType(Class.class, builder ->
				builder.withMembers(MemberCategory.INVOKE_PUBLIC_METHODS));

		// SerializationUtils for propertyToMethodInvokingFunction fallback
		reflectionHints.registerTypeIfPresent(classLoader, 
				"org.springframework.kafka.support.serializer.SerializationUtils", builder ->
				builder.withMembers(MemberCategory.INVOKE_PUBLIC_METHODS,
							MemberCategory.INVOKE_DECLARED_METHODS));

		// Common Jackson types that might be dynamically loaded
		Stream.of(
				"com.fasterxml.jackson.core.type.TypeReference",
				"com.fasterxml.jackson.databind.ObjectMapper",
				"com.fasterxml.jackson.databind.ObjectReader")
			.forEach(type -> reflectionHints.registerTypeIfPresent(classLoader, type, builder ->
					builder.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
								MemberCategory.INVOKE_PUBLIC_METHODS)));

		// Register common pattern for user-defined type resolver methods
		// These are typically static methods that return JavaType
		registerCommonTypeResolverPatterns(reflectionHints, classLoader);
	}

	/**
	 * Register hints for common type resolver patterns used in JsonDeserializer.
	 * This helps with VALUE_TYPE_METHOD and KEY_TYPE_METHOD configurations.
	 */
	private void registerCommonTypeResolverPatterns(ReflectionHints reflectionHints, @Nullable ClassLoader classLoader) {
		// Register pattern for static methods that return JavaType
		// This is a preventive measure for common type resolver patterns
		
		// If there are known type resolver classes in the classpath, register them
		// This is where users can extend to add their specific type resolvers
		
		// Example pattern: register classes that contain "TypeResolver" in their name
		// This is a common naming convention for type resolver classes
		Stream.of(
				"org.springframework.kafka.support.mapping.AbstractJavaTypeMapper",
				"org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper",
				"org.springframework.kafka.support.mapping.ClassMapper")
			.forEach(type -> reflectionHints.registerTypeIfPresent(classLoader, type, builder ->
					builder.withMembers(MemberCategory.INVOKE_DECLARED_METHODS,
								MemberCategory.INVOKE_PUBLIC_METHODS)));
	}

}
