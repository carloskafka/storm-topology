package tutorial.storm.trident.example;

import backtype.storm.spout.SchemeAsMultiScheme;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

import java.util.Properties;

public class CustomerKafkaSpout {

    public static OpaqueTridentKafkaSpout kafkaSpout() {
        BrokerHosts zk = new ZkHosts("localhost");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, IKafkaConstants.TOPIC_NAME);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf);

        return kafkaSpout;
    }

    public interface IKafkaConstants {
        public static String KAFKA_BROKERS = "localhost:9092";
        public static Integer MESSAGE_COUNT = 100;
        public static String CLIENT_ID = "client1";
        public static String TOPIC_NAME = "topicoparateste";
        public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
        public static String GROUP_ID_CONFIG = "consumerGroup";
        public static String OFFSET_RESET_LATEST = "latest";
        public static String OFFSET_RESET_EARLIER = "earliest";
        public static Integer MAX_POLL_RECORDS = 1;
    }

    static class ProducerCreator {
        public static Producer<Long, Customer> createProducer() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());
            return new KafkaProducer<>(props);
        }
    }

    public static void initializeCustomersKafkaTopic() {
        Producer<Long, Customer> producer = ProducerCreator.createProducer();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, Customer> record = new ProducerRecord<Long, Customer>(IKafkaConstants.TOPIC_NAME,
                    new Customer(1L, "1234"));
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (Exception e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
}
