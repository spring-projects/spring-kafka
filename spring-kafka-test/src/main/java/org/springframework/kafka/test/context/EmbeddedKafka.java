/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.kafka.test.context;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * Annotation that can be specified on a test class that runs Spring Kafka based tests.
 * Provides the following features over and above the regular <em>Spring TestContext
 * Framework</em>:
 * <ul>
 * <li>Registers a {@link org.springframework.kafka.test.EmbeddedKafkaBroker} bean with the
 * {@link org.springframework.kafka.test.EmbeddedKafkaBroker#BEAN_NAME} bean name.
 * </li>
 * </ul>
 * <p>
 * The typical usage of this annotation is like:
 * <pre class="code">
 * &#064;RunWith(SpringRunner.class)
 * &#064;EmbeddedKafka
 * public class MyKafkaTests {
 *
 *    &#064;Autowired
 *    private EmbeddedKafkaBroker kafkaEmbedded;
 *
 *    &#064;Value("${spring.embedded.kafka.brokers}")
 *    private String brokerAddresses;
 * }
 * </pre>
 *
 * @author Artem Bilan
 * @author Elliot Metsger
 * @author Zach Olauson
 * @author Gary Russell
 * @author Sergio Lourenco
 *
 * @since 1.3
 *
 * @see org.springframework.kafka.test.EmbeddedKafkaBroker
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface EmbeddedKafka {

	/**
	 * @return the number of brokers
	 */
	@AliasFor("count")
	int value() default 1;

	/**
	 * @return the number of brokers
	 */
	@AliasFor("value")
	int count() default 1;

	/**
	 * @return passed into {@code kafka.utils.TestUtils.createBrokerConfig()}.
	 */
	boolean controlledShutdown() default false;

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * A port must be provided for each instance, which means the number of ports must match the value of the count attribute.
	 * @return ports for brokers.
	 * @since 2.2.4
	 */
	int[] ports() default {0};

	/**
	 * @return partitions per topic
	 */
	int partitions() default 2;

	/**
	 * Topics that should be created Topics may contain property placeholders, e.g.
	 * {@code topics = "${kafka.topic.one:topicOne}"} The topics will be created with
	 * {@link #partitions()} partitions; to provision other topics with other partition
	 * counts call the {@code addTopics(NewTopic... topics)} method on the autowired
	 * broker.
	 * @return the topics to create
	 */
	String[] topics() default { };

	/**
	 * Properties in form {@literal key=value} that should be added to the broker config
	 * before runs. Properties may contain property placeholders, e.g.
	 * {@code delete.topic.enable=${topic.delete:true}}.
	 * @return the properties to add
	 * @see #brokerPropertiesLocation()
	 * @see org.springframework.kafka.test.EmbeddedKafkaBroker#brokerProperties(java.util.Map)
	 */
	String[] brokerProperties() default { };

	/**
	 * Spring {@code Resource} url specifying the location of properties that should be
	 * added to the broker config. The {@code brokerPropertiesLocation} url and the
	 * properties themselves may contain placeholders that are resolved during
	 * initialization. Properties specified by {@link #brokerProperties()} will override
	 * properties found in {@code brokerPropertiesLocation}.
	 * @return a {@code Resource} url specifying the location of properties to add
	 * @see #brokerProperties()
	 * @see org.springframework.kafka.test.EmbeddedKafkaBroker#brokerProperties(java.util.Map)
	 */
	String brokerPropertiesLocation() default "";

}

