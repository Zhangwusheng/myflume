package com.ctg.aep.kerberostest;

import com.ctg.aep.data.AEPDataObject;
import com.ctg.aep.source.kafka.AEPKafkaSourceConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public  class KafkaWlwDebug  {
    protected Properties kafkaProps;
    protected KafkaConsumer<String, byte[]> consumer;

    public void initialize() throws Exception {

        kafkaProps = new Properties();


        String KafkaGroupId = "aep-dataserver"+ System.currentTimeMillis();;

        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
        kafkaProps.put("client.id", "aep_client_1");
        kafkaProps.put("auto.commit.interval.ms", 3000);
        kafkaProps.put("bootstrap.servers", "computer0.ctcloud.com:6667,computer1.ctcloud.com:6667,computer2.ctcloud.com:6667");
        kafkaProps.put("security.protocol", "PLAINTEXT");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaGroupId);

        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_AUTO_COMMIT);
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
    }


    public void doWork() throws Exception {
        Iterator<ConsumerRecord<String, byte[]>> it;

        List<String> topics = new ArrayList<>();
        topics.add("ad");
        consumer.subscribe(topics);
        it = consumer.poll(1000).iterator();

        int i = 0;
        while (true) {
            while (it.hasNext()) {
                ConsumerRecord<String, byte[]> message = it.next();
                String kafkaKey = message.key();
                byte[] kafkaMessage = message.value();

                String strValue = new String(kafkaMessage);
                System.out.println("key=" + kafkaKey + ",value=" + strValue);
            }

            it = consumer.poll(1000).iterator();
            i++;

            if( i> 20 ){
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String value = "{\"deviceId\":\"29fba1652afc4448b2a5126552fb0cd3\"" +
                ",\"deviceType\":\"\",\"tenantId\":\"300\",\"productId\":\"1\"" +
                ",\"messageType\":\"ad\",\"topic\":\"v1/up/ad\"" +
                ",\"assocAssetId\":\"\",\"timestamp\":1519715852264" +
                ",\"payload\":{\"speed\":120.3,\"duration\":2,\"city\":\"Nanjing\"}" +
                ",\"upPacketSN\":210,\"upDataSN\":785,\"datasetId\":21" +
                ",\"protocolPublicOrPrivate\":\"private\"}";
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion( JsonSerialize.Inclusion.NON_EMPTY);
        mapper.configure( SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-ddHH:mm:ss"));


        mapper.disable( DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

        Map<String,Object> result;

        result = mapper.readValue(value.getBytes(), Map.class);
        for (Map.Entry<String, Object> stringObjectEntry : result.entrySet()) {
            System.out.println(stringObjectEntry.getKey()+"="+stringObjectEntry.getValue());
        }

        AEPDataObject aepDataObject = new AEPDataObject();
        aepDataObject.initFromMap(result,mapper);
        System.out.println(aepDataObject);

        String s= mapper.writeValueAsString(result.get("payload"));
        System.out.println(s);
//        AEPDataObject aepDataObject = mapper.readValue (value, AEPDataObject.class );
//        System.out.println( aepDataObject);

//        KafkaWlwDebug kafkaWlwDebug = new KafkaWlwDebug();
//
//        kafkaWlwDebug.initialize();
//
//        kafkaWlwDebug.doWork();
    }
}
