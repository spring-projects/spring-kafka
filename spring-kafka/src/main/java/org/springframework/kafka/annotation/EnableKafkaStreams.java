/*
 * Copyright 2016-2017 the original author or authors.
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

import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;

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
 *  public StreamsConfig kStreamsConfigs() {
 *     ...
 *  }
 * 	// other &#064;Bean definitions
 * }
 * </pre>
 *
 * That {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME} is required
 * to declare {@link StreamsBuilderFactoryBean} with the
 * {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_BUILDER_BEAN_NAME}.
 * <p>
 * Also to enable Kafka Streams feature you should be sure that the {@code kafka-streams} jar is
 * on classpath.
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
