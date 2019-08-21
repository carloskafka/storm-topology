package tutorial.storm.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import scala.actors.threadpool.Arrays;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class JoinExample {

    private static int generateRandom(List randomObjects) {
        int min = 0;
        int max = randomObjects.size() - 1;

        int currentValue = new Random().nextInt((max - min) + 1) + min;

        return currentValue;
    }

    private static void initializeCustomersFile() {
        try {
            List<Long> randomIds = Arrays.asList(new Long[]{1L, 2L});
            List<String> randomNames = Arrays.asList(new String[]{"john", "maria"});

            for (int amountOfCustomers = 0; amountOfCustomers <= 1_000_000_000; amountOfCustomers++) {
                Long randomId = randomIds.get(generateRandom(randomIds));
                String randomName = randomNames.get(generateRandom(randomNames));

                Customer customer = new Customer(randomId, randomName);

                String customerJson = new ObjectMapper().writeValueAsString(customer);
                FileUtils.writeStringToFile(new File("src/main/resources/", "customers.json"), customerJson, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class Customer {
        private Long id;
        private String nome;

        public Customer() {
        }

        public Customer(Long id, String nome) {
            this.id = id;
            this.nome = nome;
        }

        public Long getId() {
            return id;
        }

        public String getNome() {
            return nome;
        }

        @Override
        public String toString() {
            return "Customer{" +
                    "id=" + id +
                    ", nome='" + nome + '\'' +
                    '}';
        }
    }

    static class CustomerBolt extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String customerJson = (String) tuple.get(0);

            try {
                Customer customer = new ObjectMapper().readValue(customerJson, Customer.class);

                collector.emit(new Values(customer.getId(), customer));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class PrintCustomer extends BaseFilter {

        @Override
        public boolean isKeep(TridentTuple tuple) {
            // System.out.println("CustomerID: " + tuple.getValueByField("customerId").toString());
            // System.out.println("Customer: " + tuple.getValueByField("customerContent").toString());
            return true;
        }
    }

    static class CustomerStoreSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private String filePath;
        private List<String> customersJson;

        public CustomerStoreSpout(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
            this.collector = collector;
            try {
                customersJson = FileUtils.readLines(new File(filePath), Charset.forName("UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void nextTuple() {
            for (String customerJson : customersJson) {
                List<Integer> emittedValues = collector.emit(new Values(customerJson));
            }
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("customers"));
        }
    }

    public static OpaqueTridentKafkaSpout kafkaSpout() {
        TridentTopology topology = new TridentTopology();
        BrokerHosts zk = new ZkHosts("localhost");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "topicoparateste");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf);

        return kafkaSpout;
    }

    public interface IKafkaConstants {
        public static String KAFKA_BROKERS = "localhost:9092";
        public static Integer MESSAGE_COUNT = 300_000_000;
        public static String CLIENT_ID = "client1";
        public static String TOPIC_NAME = "topicoparateste";
        public static String GROUP_ID_CONFIG = "consumerGroup1";
        public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
        public static String OFFSET_RESET_LATEST = "latest";
        public static String OFFSET_RESET_EARLIER = "earliest";
        public static Integer MAX_POLL_RECORDS = 1;
    }

    static class ConsumerCreator {
        public static Consumer<Long, Customer> createConsumer() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomObjectDeserializer.class.getName());
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);
            Consumer<Long, Customer> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
            return consumer;
        }
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

    static void runConsumer() {
        Consumer<Long, Customer> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, Customer> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
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

    public static StormTopology buildTopology() {
        //initializeCustomersFile();
        //initializeCustomersKafkaTopic();
        //runConsumer();

        TridentTopology topology = new TridentTopology();

        Stream customersContent = topology
                //.newStream("customersStream", new CustomerStoreSpout("src/main/resources/customers.json"))
                .newStream("customersStream", kafkaSpout())
                // .each(new Fields("customers"), new CustomerBolt(), new Fields("customerId", "customerContent"));
                .each(new Fields("str"), new CustomerBolt(), new Fields("customerId", "customerContent"));

        Stream customersContent2 = topology
                .newStream("customersStream2", kafkaSpout())
                //.each(new Fields("customers"), new CustomerBolt(), new Fields("customerId", "customerContent"));
                .each(new Fields("str"), new CustomerBolt(), new Fields("customerId", "customerContent"));

        topology.join(customersContent, new Fields("customerId"), customersContent2, new Fields("customerId"),
                new Fields("customerId", "customerContent"))
                .each(new Fields("customerId", "customerContent"), new PrintCustomer());

        return topology.build();

    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        conf.setNumWorkers(1);

        new LocalCluster().submitTopology("topology", conf, buildTopology());
    }


}
