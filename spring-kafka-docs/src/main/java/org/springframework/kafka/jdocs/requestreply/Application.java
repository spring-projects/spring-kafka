/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.jdocs.requestreply;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyTypedMessageFuture;
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Code snippets for request/reply messaging.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
@SpringBootApplication
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        System.setProperty("spring.kafka.producer.value-serializer", ByteArraySerializer.class.getName());
        System.setProperty("spring.kafka.consumer.value-deserializer", ByteArrayDeserializer.class.getName());
        SpringApplication.run(Application.class, args).close();
    }

    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("requests")
                        .partitions(10)
                        .replicas(1)
                        .build(),
                TopicBuilder.name("replies")
                        .partitions(10)
                        .replicas(1)
                        .build());
    }

    @Bean
    KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> pf) {
        return new KafkaTemplate<>(pf);
    }

 // tag::beans[]
    @Bean
    ReplyingKafkaTemplate<String, String, String> template(
            ProducerFactory<String, String> pf,
            ConcurrentKafkaListenerContainerFactory<String, String> factory) {

        ConcurrentMessageListenerContainer<String, String> replyContainer =
                factory.createContainer("replies");
        replyContainer.getContainerProperties().setGroupId("request.replies");
        ReplyingKafkaTemplate<String, String, String> template =
                new ReplyingKafkaTemplate<>(pf, replyContainer);
        template.setMessageConverter(new ByteArrayJsonMessageConverter());
        template.setDefaultTopic("requests");
        return template;
    }
 // end::beans[]

    @Bean
    ApplicationRunner runner(ReplyingKafkaTemplate<String, String, String> template) {
        return args -> {
// tag::sendReceive[]
            RequestReplyTypedMessageFuture<String, String, Thing> future1 =
                    template.sendAndReceive(MessageBuilder.withPayload("getAThing").build(),
                            new ParameterizedTypeReference<Thing>() { });
            log.info(future1.getSendFuture().get(10, TimeUnit.SECONDS).getRecordMetadata().toString());
            Thing thing = future1.get(10, TimeUnit.SECONDS).getPayload();
            log.info(thing.toString());

            RequestReplyTypedMessageFuture<String, String, List<Thing>> future2 =
                    template.sendAndReceive(MessageBuilder.withPayload("getThings").build(),
                            new ParameterizedTypeReference<List<Thing>>() { });
            log.info(future2.getSendFuture().get(10, TimeUnit.SECONDS).getRecordMetadata().toString());
            List<Thing> things = future2.get(10, TimeUnit.SECONDS).getPayload();
            things.forEach(thing1 -> log.info(thing1.toString()));
// end::sendReceive[]
        };
    }

    @KafkaListener(id = "myId", topics = "requests", properties = "auto.offset.reset:earliest")
    @SendTo
    public byte[] listen(String in) {
        log.info(in);
        if (in.equals("\"getAThing\"")) {
            return ("{\"thingProp\":\"someValue\"}").getBytes();
        }
        if (in.equals("\"getThings\"")) {
            return ("[{\"thingProp\":\"someValue1\"},{\"thingProp\":\"someValue2\"}]").getBytes();
        }
        return in.toUpperCase().getBytes();
    }

    public static class Thing {

        private String thingProp;

        public String getThingProp() {
            return this.thingProp;
        }

        public void setThingProp(String thingProp) {
            this.thingProp = thingProp;
        }

        @Override
        public String toString() {
            return "Thing [thingProp=" + this.thingProp + "]";
        }

    }

}


