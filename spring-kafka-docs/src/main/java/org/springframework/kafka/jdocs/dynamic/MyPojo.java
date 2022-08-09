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

import org.springframework.kafka.annotation.KafkaListener;

/**
 * Pojo for dynamic listener creation.
 * @author Gary Russell
 * @since 2.8.9
 *
 */
//tag::pojo[]

public class MyPojo {

    private final String id;

    private final String topic;

    public MyPojo(String id, String topic) {
        this.id = id;
        this.topic = topic;
    }

    public String getId() {
        return this.id;
    }

    public String getTopic() {
        return this.topic;
    }

    @KafkaListener(id = "#{__listener.id}", topics = "#{__listener.topic}")
    public void listen(String in) {
        System.out.println(in);
    }

}
// end::pojo[]
