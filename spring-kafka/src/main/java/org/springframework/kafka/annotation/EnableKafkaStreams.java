/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

/**
 * Enable default Kafka Streams components. To be used on
 * {@link org.springframework.context.annotation.Configuration Configuration} classes as
 * follows:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableKafkaStreams
 * public class AppConfig {
 *
 * 	&#064;Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
 * 	public KafkaStreamsConfiguration kStreamsConfigs() {
 * 		...
 * 	}
 * 	// other &#064;Bean definitions
 * }
 * </pre>
 *
 * That {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME} is
 * required to declare {@link org.springframework.kafka.config.StreamsBuilderFactoryBean}
 * with the {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_BUILDER_BEAN_NAME}.
 * <p>
 * Also to enable Kafka Streams feature you should be sure that the {@code kafka-streams}
 * jar is on classpath.
 *
 * @author Artem Bilan
 *
 * @since 1.1.4
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KafkaStreamsDefaultConfiguration.class)
public @interface EnableKafkaStreams {
}
