package tutorial.storm.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import scala.actors.threadpool.Arrays;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

    public static StormTopology buildTopology() {
        // initializeCustomersFile();

        TridentTopology topology = new TridentTopology();

        Stream customersContent = topology
                .newStream("customersStream", new CustomerStoreSpout("src/main/resources/customers.json"))
                .each(new Fields("customers"), new CustomerBolt(), new Fields("customerId", "customerContent"));

        Stream customersContent2 = topology
                .newStream("customersStream2", new CustomerStoreSpout("src/main/resources/customers.json"))
                .each(new Fields("customers"), new CustomerBolt(), new Fields("customerId", "customerContent"));

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
