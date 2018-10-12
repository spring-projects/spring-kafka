/*
 * Copyright 2016-2018 the original author or authors.
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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

/**
 * Enable Kafka listener annotated endpoints that are created under the covers by a
 * {@link org.springframework.kafka.config.AbstractKafkaListenerContainerFactory
 * AbstractListenerContainerFactory}. To be used on
 * {@link org.springframework.context.annotation.Configuration Configuration} classes as
 * follows:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig {
 * 	&#064;Bean
 * 	public ConcurrentKafkaListenerContainerFactory myKafkaListenerContainerFactory() {
 * 		ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
 * 		factory.setConsumerFactory(consumerFactory());
 * 		factory.setConcurrency(4);
 * 		return factory;
 * 	}
 * 	// other &#064;Bean definitions
 * }
 * </pre>
 *
 * The {@code KafkaListenerContainerFactory} is responsible to create the listener
 * container for a particular endpoint. Typical implementations, as the
 * {@link org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
 * ConcurrentKafkaListenerContainerFactory} used in the sample above, provides the
 * necessary configuration options that are supported by the underlying
 * {@link org.springframework.kafka.listener.MessageListenerContainer
 * MessageListenerContainer}.
 *
 * <p>
 * {@code @EnableKafka} enables detection of {@link KafkaListener} annotations on any
 * Spring-managed bean in the container. For example, given a class {@code MyService}:
 *
 * <pre class="code">
 * package com.acme.foo;
 *
 * public class MyService {
 * 	&#064;KafkaListener(containerFactory = "myKafkaListenerContainerFactory", topics = "myTopic")
 * 	public void process(String msg) {
 * 		// process incoming message
 * 	}
 * }
 * </pre>
 *
 * The container factory to use is identified by the
 * {@link KafkaListener#containerFactory() containerFactory} attribute defining the name
 * of the {@code KafkaListenerContainerFactory} bean to use. When none is set a
 * {@code KafkaListenerContainerFactory} bean with name
 * {@code kafkaListenerContainerFactory} is assumed to be present.
 *
 * <p>
 * the following configuration would ensure that every time a message is received from
 * topic "myQueue", {@code MyService.process()} is called with the content of the message:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig {
 * 	&#064;Bean
 * 	public MyService myService() {
 * 		return new MyService();
 * 	}
 *
 * 	// Kafka infrastructure setup
 * }
 * </pre>
 *
 * Alternatively, if {@code MyService} were annotated with {@code @Component}, the
 * following configuration would ensure that its {@code @KafkaListener} annotated method
 * is invoked with a matching incoming message:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableKafka
 * &#064;ComponentScan(basePackages = "com.acme.foo")
 * public class AppConfig {
 * }
 * </pre>
 *
 * Note that the created containers are not registered with the application context but
 * can be easily located for management purposes using the
 * {@link org.springframework.kafka.config.KafkaListenerEndpointRegistry
 * KafkaListenerEndpointRegistry}.
 *
 * <p>
 * Annotated methods can use a flexible signature; in particular, it is possible to use
 * the {@link org.springframework.messaging.Message Message} abstraction and related
 * annotations, see {@link KafkaListener} Javadoc for more details. For instance, the
 * following would inject the content of the message and the kafka partition header:
 *
 * <pre class="code">
 * &#064;KafkaListener(containerFactory = "myKafkaListenerContainerFactory", topics = "myTopic")
 * public void process(String msg, @Header("kafka_partition") int partition) {
 * 	// process incoming message
 * }
 * </pre>
 *
 * These features are abstracted by the
 * {@link org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory
 * MessageHandlerMethodFactory} that is responsible to build the necessary invoker to
 * process the annotated method. By default,
 * {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory
 * DefaultMessageHandlerMethodFactory} is used.
 *
 * <p>
 * When more control is desired, a {@code @Configuration} class may implement
 * {@link KafkaListenerConfigurer}. This allows access to the underlying
 * {@link org.springframework.kafka.config.KafkaListenerEndpointRegistrar
 * KafkaListenerEndpointRegistrar} instance. The following example demonstrates how to
 * specify an explicit default {@code KafkaListenerContainerFactory}
 *
 * <pre class="code">
 * {
 * 	&#64;code
 * 	&#064;Configuration
 * 	&#064;EnableKafka
 * 	public class AppConfig implements KafkaListenerConfigurer {
 * 		&#064;Override
 * 		public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
 * 			registrar.setContainerFactory(myKafkaListenerContainerFactory());
 * 		}
 *
 * 		&#064;Bean
 * 		public KafkaListenerContainerFactory&lt;?, ?&gt; myKafkaListenerContainerFactory() {
 * 			// factory settings
 * 		}
 *
 * 		&#064;Bean
 * 		public MyService myService() {
 * 			return new MyService();
 * 		}
 * 	}
 * }
 * </pre>
 *
 * It is also possible to specify a custom
 * {@link org.springframework.kafka.config.KafkaListenerEndpointRegistry
 * KafkaListenerEndpointRegistry} in case you need more control on the way the containers
 * are created and managed. The example below also demonstrates how to customize the
 * {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory}
 * as well as how to supply a custom {@link org.springframework.validation.Validator
 * Validator} so that payloads annotated with
 * {@link org.springframework.validation.annotation.Validated Validated} are first
 * validated against a custom {@code Validator}.
 *
 * <pre class="code">
 * {
 * 	&#64;code
 * 	&#064;Configuration
 * 	&#064;EnableKafka
 * 	public class AppConfig implements KafkaListenerConfigurer {
 * 		&#064;Override
 * 		public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
 * 			registrar.setEndpointRegistry(myKafkaListenerEndpointRegistry());
 * 			registrar.setMessageHandlerMethodFactory(myMessageHandlerMethodFactory);
 * 			registrar.setValidator(new MyValidator());
 * 		}
 *
 * 		&#064;Bean
 * 		public KafkaListenerEndpointRegistry myKafkaListenerEndpointRegistry() {
 * 			// registry configuration
 * 		}
 *
 * 		&#064;Bean
 * 		public MessageHandlerMethodFactory myMessageHandlerMethodFactory() {
 * 			DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
 * 			// factory configuration
 * 			return factory;
 * 		}
 *
 * 		&#064;Bean
 * 		public MyService myService() {
 * 			return new MyService();
 * 		}
 * 	}
 * }
 * </pre>
 *
 * Implementing {@code KafkaListenerConfigurer} also allows for fine-grained control over
 * endpoints registration via the {@code KafkaListenerEndpointRegistrar}. For example, the
 * following configures an extra endpoint:
 *
 * <pre class="code">
 * {
 * 	&#64;code
 * 	&#064;Configuration
 * 	&#064;EnableKafka
 * 	public class AppConfig implements KafkaListenerConfigurer {
 * 		&#064;Override
 * 		public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
 * 			SimpleKafkaListenerEndpoint myEndpoint = new SimpleKafkaListenerEndpoint();
 * 			// ... configure the endpoint
 * 			registrar.registerEndpoint(endpoint, anotherKafkaListenerContainerFactory());
 * 		}
 *
 * 		&#064;Bean
 * 		public MyService myService() {
 * 			return new MyService();
 * 		}
 *
 * 		&#064;Bean
 * 		public KafkaListenerContainerFactory&lt;?, ?&gt; anotherKafkaListenerContainerFactory() {
 * 			// ...
 * 		}
 *
 * 		// Kafka infrastructure setup
 * 	}
 * }
 * </pre>
 *
 * Note that all beans implementing {@code KafkaListenerConfigurer} will be detected and
 * invoked in a similar fashion. The example above can be translated in a regular bean
 * definition registered in the context in case you use the XML configuration.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @see KafkaListener
 * @see KafkaListenerAnnotationBeanPostProcessor
 * @see org.springframework.kafka.config.KafkaListenerEndpointRegistrar
 * @see org.springframework.kafka.config.KafkaListenerEndpointRegistry
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KafkaBootstrapConfiguration.class)
public @interface EnableKafka {
}
