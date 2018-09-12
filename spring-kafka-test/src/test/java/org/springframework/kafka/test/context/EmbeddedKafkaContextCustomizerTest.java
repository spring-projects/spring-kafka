/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.test.context;

import org.junit.Before;
import org.junit.Test;
import org.springframework.core.annotation.AnnotationUtils;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;


/**
 * @author Oleg Artyomov
 * @since 1.3
 */

public class EmbeddedKafkaContextCustomizerTest {

    private EmbeddedKafka annotationFromFirstClass;
    private EmbeddedKafka annotationFromSecondClass;


    @Before
    public void beforeEachTest() {
        annotationFromFirstClass = AnnotationUtils.findAnnotation(TestWithEmbeddedKafka.class, EmbeddedKafka.class);
        annotationFromSecondClass = AnnotationUtils.findAnnotation(SecondTestWithEmbeddedKafka.class, EmbeddedKafka.class);
    }


    @Test
    public void testHashCode() {
        assertThat(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass).hashCode(), not(equalTo(0)));
        assertEquals(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass).hashCode(), new EmbeddedKafkaContextCustomizer(annotationFromSecondClass).hashCode());
    }


    @Test
    public void testEquals() {
        assertEquals(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass), new EmbeddedKafkaContextCustomizer(annotationFromSecondClass));
        assertNotEquals(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass), new Object());
    }


    @EmbeddedKafka
    private class TestWithEmbeddedKafka {

    }

    @EmbeddedKafka
    private class SecondTestWithEmbeddedKafka {

    }

}