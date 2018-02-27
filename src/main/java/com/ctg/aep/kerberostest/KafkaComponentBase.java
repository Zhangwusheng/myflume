package com.ctg.aep.kerberostest;

import com.ctg.aep.source.kafka.AEPKafkaSourceConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public abstract class KafkaComponentBase extends BaseComponent {
    protected Properties kafkaProps;
    protected KafkaConsumer<String, byte[]> consumer;

    protected abstract void doInit() throws Exception;

    @Override
    public void init() throws Exception {
        doInit();

        kafkaProps = new Properties();

        String KafkaGroupId = "aep-dataserver";

        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
        kafkaProps.put("client.id", "aep_client_1");
        kafkaProps.put("auto.commit.interval.ms", 3000);
        kafkaProps.put("bootstrap.servers", "danalysis.dfs.com:6667,danalysis2.dfs.com:6667,danalysis1.dfs.com:6667");
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaGroupId);

        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_AUTO_COMMIT);
        consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
    }

    @Override
    public void work() throws Exception {
        Iterator<ConsumerRecord<String, byte[]>> it;

        List<String> topics = new ArrayList<>();
        topics.add("test");
        consumer.subscribe(topics);
        it = consumer.poll(1000).iterator();

        while (true) {
            while (it.hasNext()) {
                ConsumerRecord<String, byte[]> message = it.next();
                String kafkaKey = message.key();
                byte[] kafkaMessage = message.value();

                String strValue = new String(kafkaMessage);
                System.out.println("key=" + kafkaKey + ",value=" + strValue);
            }

            it = consumer.poll(1000).iterator();
        }
    }
}
