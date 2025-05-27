package com.recargapay.wallet.query.service;

import com.mongodb.client.MongoClient;
import com.recargapay.wallet.query.config.TestMongoConfig;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = { TestMongoConfig.class })
@ActiveProfiles("integration")
class MongoConnectionIntegrationTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoClient mongoClient;

    @BeforeEach
    void setUp() {
        // Clear the test database before each test
        mongoTemplate.getDb().drop();
        System.out.println("MongoDB database dropped");
    }

    @Test
    void testMongoConnection_InsertAndRetrieve_Success() {
        // Arrange
        String collectionName = "test_collection";
        Document document = new Document("key", "value");

        // Act
        mongoTemplate.save(document, collectionName);

        // Assert
        Document retrieved = mongoTemplate.findOne(new Query(Criteria.where("key").is("value")), Document.class, collectionName);
        assertNotNull(retrieved, "Document should be retrieved");
        assertEquals("value", retrieved.getString("key"), "Document key should match");
    }

    @Test
    void testMongoClient_ConnectionAlive() {
        // Act & Assert
        assertNotNull(mongoClient, "MongoClient should not be null");
        assertDoesNotThrow(() -> {
            mongoClient.listDatabaseNames().first();
        }, "Should be able to list database names without exception");
    }
}