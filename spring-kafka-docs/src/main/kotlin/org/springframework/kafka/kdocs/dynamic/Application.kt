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
package org.springframework.kafka.kdocs.dynamic

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Scope
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin.NewTopics
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.MessageListener

/**
 * @author Gary Russell
 * @since 2.8.9
 */
@SpringBootApplication
class Application {
    @Bean
    fun runner(factory: ConcurrentKafkaListenerContainerFactory<String, String>): ApplicationRunner {
        return ApplicationRunner { args: ApplicationArguments? -> createContainer(factory, "topic1", "group1") }
    }

    @Bean
    fun runner(applicationContext: ApplicationContext): ApplicationRunner {
        return ApplicationRunner { args: ApplicationArguments? ->
// tag::getBeans[]

applicationContext.getBean(MyPojo::class.java, "one", arrayOf("topic2"))
applicationContext.getBean(MyPojo::class.java, "two", arrayOf("topic3"))
// end::getBeans[]
        }
    }

// tag::create[]

private fun createContainer(
    factory: ConcurrentKafkaListenerContainerFactory<String, String>, topic: String, group: String
): ConcurrentMessageListenerContainer<String, String> {
    val container = factory.createContainer(topic)
    container.containerProperties.messageListener = MyListener()
    container.containerProperties.groupId = group
    container.beanName = group
    container.start()
    return container
}
// end::create[]
    @Bean
    fun topics(): NewTopics {
        return NewTopics(
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
                .build()
        )
    }

// tag::pojoBean[]

@Bean
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
fun pojo(id: String?, topic: String?): MyPojo {
    return MyPojo(id, topic)
}
//end::pojoBean[]

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(Application::class.java, *args).close()
        }
    }
}

// tag::listener[]

class MyListener : MessageListener<String?, String?> {

    override fun onMessage(data: ConsumerRecord<String?, String?>) {
        // ...
    }

}
// end::listener[]

// tag::pojo[]

class MyPojo(id: String?, topic: String?) {

    @KafkaListener(id = "#{__listener.id}", topics = ["#{__listener.topics}"])
    fun listen(`in`: String?) {
        println(`in`)
    }

}
// end::pojo[]
