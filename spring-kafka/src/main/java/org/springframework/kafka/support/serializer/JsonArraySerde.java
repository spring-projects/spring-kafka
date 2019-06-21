package org.springframework.kafka.support.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;

public class JsonArraySerde<T> implements Serde<List<T>> {
    private final Serializer<List<T>> jsonArraySerializer;
    private final Deserializer<List<T>> jsonArrayDeserializer;

    public JsonArraySerde(Class<T> targetType) {
        var objectMapper = new ObjectMapper();

        this.jsonArraySerializer = new JsonArraySerializer<>(objectMapper);
        this.jsonArrayDeserializer = new JsonArrayDeserializer<>(targetType, objectMapper);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<List<T>> serializer() {
        return jsonArraySerializer;
    }

    @Override
    public Deserializer<List<T>> deserializer() {
        return jsonArrayDeserializer;
    }
}