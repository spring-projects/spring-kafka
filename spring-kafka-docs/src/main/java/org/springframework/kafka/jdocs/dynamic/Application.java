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

package org.springframework.kafka.jdocs.dynamic;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/**
 * Dynamic listeners.
 *
 * @author Gary Russell
 * @since 2.8.9
 *
 */
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    ApplicationRunner runner(ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        return args -> {
            createContainer(factory, "topic1", "group1");
        };
    }

    @Bean
    public ApplicationRunner runner1(ApplicationContext applicationContext) {
        return args -> {
// tag::getBeans[]

applicationContext.getBean(MyPojo.class, "one", "topic2");
applicationContext.getBean(MyPojo.class, "two", "topic3");
// end::getBeans[]
        };
    }


// tag::create[]

private ConcurrentMessageListenerContainer<String, String> createContainer(
        ConcurrentKafkaListenerContainerFactory<String, String> factory, String topic, String group) {

    ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(topic);
    container.getContainerProperties().setMessageListener(new MyListener());
    container.getContainerProperties().setGroupId(group);
    container.setBeanName(group);
    container.start();
    return container;
}
// end::create[]
@Bean
public KafkaAdmin.NewTopics topics() {
    return new KafkaAdmin.NewTopics(
            TopicBuilder.name("topic1")
                    .partitions(10)
                    .replicas(1)
                    .build(),
            TopicBuilder.name("topic2")
                    .partitions(10)
                    .replicas(1)
                    .build(),
            TopicBuilder.name("topic3")
                    .partitions(10)
                    .replicas(1)
                    .build());
}

// tag::pojoBean[]

@Bean
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
MyPojo pojo(String id, String topic) {
	return new MyPojo(id, topic);
}
//end::pojoBean[]

}
