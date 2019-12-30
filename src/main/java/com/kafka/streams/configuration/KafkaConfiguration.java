package com.kafka.streams.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.kafka.streams.events.DomainEvent;
import com.kafka.streams.model.CreditCard;
import com.kafka.streams.repository.Repository;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfiguration {

    public static final String S1P_SNAPSHOTS_FOR_CARDS = "s1p-snapshots-for-cards";

    @Value("${streams.app.id}")
    private String appId;

    @Value("${streams.kafka.brokers}")
    private String kafkaBrokers;

    @Bean
    KafkaTemplate<String, DomainEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    ProducerFactory<String, DomainEvent> producerFactory() {
        return new DefaultKafkaProducerFactory<>(config());
    }

    private Map<String, Object> config() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return config;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration streamsConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        return new KafkaStreamsConfiguration(config);
    }

    @Bean
    KTable<String, CreditCard> kTable(StreamsBuilder builder) {
        Serde<DomainEvent> domainEventSerde = new JsonSerde<>(DomainEvent.class);
        Serde<CreditCard> creditCardSerde = new JsonSerde<>(CreditCard.class);

        Aggregator<String, DomainEvent, CreditCard> ag = (String s, DomainEvent domainEvent,
                CreditCard creditCard) -> creditCard.handle(domainEvent);
        Initializer<CreditCard> in = () -> new CreditCard();

        Materialized<String, CreditCard, KeyValueStore<Bytes, byte[]>> ma = Materialized
                .<String, CreditCard, KeyValueStore<Bytes, byte[]>>as(S1P_SNAPSHOTS_FOR_CARDS)
                .withKeySerde(Serdes.String()).withValueSerde(creditCardSerde);

        return builder.stream(Repository.S1P_CREDIT_CARDS_EVENTS, Consumed.with(Serdes.String(), domainEventSerde))
                .groupBy((s, domainEvent) -> domainEvent.aggregateUUID().toString()).aggregate(in, ag, ma);
    }
}
