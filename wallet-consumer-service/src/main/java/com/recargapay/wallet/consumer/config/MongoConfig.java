package com.recargapay.wallet.consumer.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.UuidRepresentation; // Importação correta
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Configures MongoDB connection and transaction management for the wallet service.
 */
@Configuration
public class MongoConfig extends AbstractMongoClientConfiguration {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.data.mongodb.database}")
    private String databaseName;

    /**
     * Configures the MongoClient with a custom codec for OffsetDateTime and UUID representation.
     *
     * @return Configured MongoClient
     */
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
                .uuidRepresentation(UuidRepresentation.STANDARD) // Corrigido
                .build();

        return MongoClients.create(clientSettings);
    }

    /**
     * Specifies the MongoDB database name.
     *
     * @return Database name
     */
    @Override
    protected String getDatabaseName() {
        return databaseName;
    }

    /**
     * Configures the MongoTemplate for MongoDB operations.
     *
     * @param databaseFactory MongoDatabaseFactory for database access
     * @param converter MappingMongoConverter for object mapping
     * @return Configured MongoTemplate
     */
    @Bean
    public MongoTemplate mongoTemplate(MongoDatabaseFactory databaseFactory, MappingMongoConverter converter) {
        return new MongoTemplate(databaseFactory, converter);
    }

    /**
     * Configures the MongoTransactionManager for transaction support.
     *
     * @param databaseFactory MongoDatabaseFactory for database access
     * @return Configured MongoTransactionManager
     */
    @Bean
    public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory databaseFactory) {
        return new MongoTransactionManager(databaseFactory);
    }

    /**
     * Configures the MappingMongoConverter for custom conversions.
     *
     * @param context MongoMappingContext for mapping configuration
     * @return Configured MappingMongoConverter
     */
    @Bean
    public MappingMongoConverter mappingMongoConverter(MongoMappingContext context) {
        MappingMongoConverter converter = new MappingMongoConverter(
                new org.springframework.data.mongodb.core.convert.DefaultDbRefResolver(mongoDbFactory()),
                context);
        converter.setCustomConversions(customConversions());
        return converter;
    }
}