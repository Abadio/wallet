package com.recargapay.wallet.query.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

@Configuration
public class MongoConfig extends AbstractMongoClientConfiguration {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.data.mongodb.database}")
    private String databaseName;

    @Override
    public MongoClient mongoClient() {
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(new OffsetDateTimeCodec()),
                MongoClientSettings.getDefaultCodecRegistry()
        );

        ConnectionString connectionString = new ConnectionString(mongoUri);
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .codecRegistry(codecRegistry)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build();

        return MongoClients.create(clientSettings);
    }

    @Override
    protected String getDatabaseName() {
        return databaseName;
    }

    @Bean
    public MongoTemplate mongoTemplate() throws Exception {
        MongoTemplate template = new MongoTemplate(mongoDbFactory(), mappingMongoConverter());
        return template;
    }

    @Bean
    public MappingMongoConverter mappingMongoConverter() throws Exception {
        MappingMongoConverter converter = new MappingMongoConverter(
                new org.springframework.data.mongodb.core.convert.DefaultDbRefResolver(mongoDbFactory()),
                new org.springframework.data.mongodb.core.mapping.MongoMappingContext());
        converter.setCustomConversions(customConversions());
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