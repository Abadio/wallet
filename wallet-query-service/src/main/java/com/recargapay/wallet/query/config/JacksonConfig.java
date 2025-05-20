package com.recargapay.wallet.query.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures Jackson ObjectMapper for JSON serialization and deserialization.
 * Ensures proper handling of Java 8 date/time types and relaxed deserialization rules.
 */
@Configuration
public class JacksonConfig {

    /**
     * Creates a configured ObjectMapper bean.
     *
     * @return ObjectMapper with JavaTimeModule and disabled timestamps
     */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }
}