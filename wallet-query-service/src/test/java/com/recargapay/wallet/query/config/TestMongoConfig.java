package com.recargapay.wallet.query.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

@Testcontainers
@Configuration
@Profile("integration")
public class TestMongoConfig {

    @Value("${spring.data.mongodb.database}")
    private String databaseName;

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:6.0.8")
            .withExposedPorts(27017)
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(512L * 1024 * 1024)) // 512MB
            .withReuse(true);

    @Bean
    public MongoDBContainer mongoDBContainer() {
        if (!mongoDBContainer.isRunning()) {
            mongoDBContainer.start();
            System.out.println("MongoDB container started on port: " + mongoDBContainer.getMappedPort(27017));
        }
        return mongoDBContainer;
    }

    @Bean
    public MongoClient mongoClient(MongoDBContainer mongoDBContainer) {
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(new OffsetDateTimeCodec()),
                MongoClientSettings.getDefaultCodecRegistry()
        );

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoDBContainer.getReplicaSetUrl()))
                .codecRegistry(codecRegistry)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build();

        return MongoClients.create(clientSettings);
    }

    @Bean
    public MongoTemplate mongoTemplate(MongoClient mongoClient, MappingMongoConverter mappingMongoConverter) {
        SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(mongoClient, databaseName);
        return new MongoTemplate(factory, mappingMongoConverter);
    }

    @Bean
    public MappingMongoConverter mappingMongoConverter(MongoClient mongoClient) {
        SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(mongoClient, databaseName);
        MappingMongoConverter converter = new MappingMongoConverter(
                new org.springframework.data.mongodb.core.convert.DefaultDbRefResolver(factory),
                new org.springframework.data.mongodb.core.mapping.MongoMappingContext());
        converter.setCustomConversions(customConversions());
        converter.afterPropertiesSet(); // Ensure conversions are initialized
        return converter;
    }

    @Bean
    public MongoCustomConversions customConversions() {
        return new MongoCustomConversions(Arrays.asList(
                new StringToOffsetDateTimeConverter(),
                new OffsetDateTimeToStringConverter()
        ));
    }

    static class StringToOffsetDateTimeConverter implements Converter<String, OffsetDateTime> {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

        @Override
        public OffsetDateTime convert(String source) {
            return source != null ? OffsetDateTime.parse(source, FORMATTER) : null;
        }
    }

    static class OffsetDateTimeToStringConverter implements Converter<OffsetDateTime, String> {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

        @Override
        public String convert(OffsetDateTime source) {
            return source != null ? source.format(FORMATTER) : null;
        }
    }
}