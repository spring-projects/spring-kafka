/*
 * Copyright 2017 the original author or authors.
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
import org.springframework.kafka.test.rule.KafkaEmbedded;

/**
 * Annotation that can be specified on a test class that runs Spring Kafka based tests.
 * Provides the following features over and above the regular <em>Spring TestContext
 * Framework</em>:
 * <ul>
 * <li>Registers a {@link KafkaEmbedded} bean with the {@link KafkaEmbedded#BEAN_NAME} bean name.
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
 *    private KafkaEmbedded kafkaEmbedded;
 *
 *    &#064;Value("${spring.embedded.kafka.brokers}")
 *    private String brokerAddresses;
 * }
 * </pre>
 *
 * @author Artem Bilan
 * @author Elliot Metsger
 *
 * @since 1.3
 *
 * @see KafkaEmbedded
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
	 * @return partitions per topic
	 */
	int partitions() default 2;

	/**
	 * @return the topics to create
	 */
	String[] topics() default { };

	/**
	 * Properties in form {@literal key=value} that should be added
	 * to the broker config before runs.
	 * Properties may contain property placeholders, e.g. {@code delete.topic.enable=${topic.delete:true}}.
	 * @return the properties to add
	 * @see #brokerPropertiesLocation()
	 * @see KafkaEmbedded#brokerProperties(java.util.Map)
	 */
	String[] brokerProperties() default { };

	/**
	 * Spring {@code Resource} url specifying the location of properties that should be
	 * added to the broker config.
	 * The {@code brokerPropertiesLocation} url and the properties themselves may contain
	 * placeholders that are resolved during initialization.
	 * Properties specified by {@link #brokerProperties()} will override properties found
	 * in {@code brokerPropertiesLocation}.
	 * @return a {@code Resource} url specifying the location of properties to add
	 * @see #brokerProperties()
	 * @see KafkaEmbedded#brokerProperties(java.util.Map)
	 */
	String brokerPropertiesLocation() default "";

}

