package com.dsehrawat.punctuator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;


import java.time.Duration;
import java.util.Properties;

public class PunctuatorExample {

    public static final String STORE_NAME = "Counts";

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(PunctuatorExample.STORE_NAME),
                        Serdes.String(), Serdes.Long()
                ));
        KStream<String, String> stream = builder.stream("quickstart-events");
        stream.transform(MyProcessor::new, STORE_NAME).foreach((key, value) -> System.out.println("Hello, " + value + "!"));
//        stream.foreach((key, value) -> System.out.println("Hello, " + value + "!"));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Void().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static class MyProcessor implements Transformer<String, String, KeyValue<String, String>>, Punctuator {
        private ProcessorContext context;
        private KeyValueStore<String, Long> kvStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            // keep the processor context locally because we need it in punctuate() and commit()
            this.context = context;

            // retrieve the key-value store named "Counts"
            this.kvStore = (KeyValueStore<String, Long>) context.getStateStore(PunctuatorExample.STORE_NAME);

            // call this processor's punctuate() method every 1000 milliseconds.
            this.context.schedule(Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            String[] words = value.toLowerCase().split(" ");

            for (String word : words) {
                Long oldValue = this.kvStore.get(word);

                if (oldValue == null) {
                    this.kvStore.put(word, 1L);
                } else {
                    this.kvStore.put(word, oldValue + 1L);
                }
            }
            return new KeyValue<>(key, value);
        }

        @Override
        public void punctuate(long timestamp) {
            KeyValueIterator<String, Long> iter = this.kvStore.all();

            while (iter.hasNext()) {
                KeyValue<String, Long> entry = iter.next();
                context.forward(entry.key, entry.value.toString());
            }

            iter.close();
            // commit the current processing progress
            context.commit();
        }

        @Override
        public void close() {
            // close any resources managed by this processor.
            // Note: Do not close any StateStores as these are managed
            // by the library
        }
    }

    ;
}
