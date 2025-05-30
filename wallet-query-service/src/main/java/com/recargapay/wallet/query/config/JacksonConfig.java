package com.recargapay.wallet.query.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.List;

@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Enable type information for deserialization
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfSubType("com.recargapay.wallet.query.document") // Allow document classes
                .allowIfSubType("com.recargapay.wallet.common.event") // Allow event classes
                .allowIfSubType(BigDecimal.class) // Allow BigDecimal
                .allowIfSubType(List.class) // Allow List implementations
                .allowIfSubType("java.util") // Allow other java.util collections
                .build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);

        return mapper;
    }
}