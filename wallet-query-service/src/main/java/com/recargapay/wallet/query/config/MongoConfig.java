package com.recargapay.wallet.query.config;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.mongodb.MongoClientSettings;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@Configuration
public class MongoConfig {

    @Bean
    public MongoClientSettings mongoClientSettings() {
        System.out.println("Configuring MongoClientSettings with OffsetDateTimeCodec for all profiles");
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(new OffsetDateTimeCodec()),
                MongoClientSettings.getDefaultCodecRegistry()
        );

        return MongoClientSettings.builder()
                .codecRegistry(codecRegistry)
                .build();
    }

    static class OffsetDateTimeCodec implements Codec<OffsetDateTime> {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

        @Override
        public void encode(BsonWriter writer, OffsetDateTime value, EncoderContext encoderContext) {
            writer.writeString(value.format(FORMATTER));
        }

        @Override
        public OffsetDateTime decode(BsonReader reader, DecoderContext decoderContext) {
            return OffsetDateTime.parse(reader.readString(), FORMATTER);
        }

        @Override
        public Class<OffsetDateTime> getEncoderClass() {
            return OffsetDateTime.class;
        }
    }
}