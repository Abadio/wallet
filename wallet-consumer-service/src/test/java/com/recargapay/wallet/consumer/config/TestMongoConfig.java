package com.recargapay.wallet.consumer.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.UuidRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * Configures MongoDB transaction management and repository support for integration tests.
 */
@Configuration
@EnableMongoRepositories(basePackages = "com.recargapay.wallet.consumer.repository")
public class TestMongoConfig {
    private static final Logger logger = LoggerFactory.getLogger(TestMongoConfig.class);

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.data.mongodb.database:wallet_service}")
    private String databaseName;

    /**
     * Creates a MongoClient with UUID representation configured.
     *
     * @return MongoClient
     */
    @Bean
    public MongoClient mongoClient() {
        ConnectionString connectionString = new ConnectionString(mongoUri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build();
        logger.debug("Creating MongoClient with URI: {}, UUID Representation: {}", mongoUri, UuidRepresentation.STANDARD);
        return MongoClients.create(settings);
    }

    /**
     * Creates a MongoDatabaseFactory for MongoDB using the configured MongoClient.
     *
     * @return MongoDatabaseFactory
     */
    @Bean
    public MongoDatabaseFactory mongoDatabaseFactory() {
        return new SimpleMongoClientDatabaseFactory(mongoClient(), databaseName);
    }

    /**
     * Creates a MongoTemplate for MongoDB operations.
     *
     * @param mongoDatabaseFactory The MongoDB database factory
     * @return MongoTemplate
     */
    @Bean
    public MongoTemplate mongoTemplate(MongoDatabaseFactory mongoDatabaseFactory) {
        return new MongoTemplate(mongoDatabaseFactory);
    }

    /**
     * Creates a MongoTransactionManager for managing transactions in MongoDB during tests.
     *
     * @param mongoDatabaseFactory The MongoDB database factory
     * @return Configured MongoTransactionManager
     */
    @Bean(name = "mongoTransactionManager")
    @Primary
    public org.springframework.data.mongodb.MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory mongoDatabaseFactory) {
        return new org.springframework.data.mongodb.MongoTransactionManager(mongoDatabaseFactory);
    }
}