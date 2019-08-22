package tutorial.storm.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;

public class JoinExample {

    public static StormTopology buildTopology() {
        //CustomerFileSpout.initializeCustomersFile();
        CustomerKafkaSpout.initializeCustomersKafkaTopic();
        //runConsumer();

        TridentTopology topology = new TridentTopology();
        Stream customersContent = topology
                //.newStream("customersStream", new CustomerStoreSpout("src/main/resources/customers.json"))
                .newStream("customersStream", CustomerKafkaSpout.kafkaSpout())
                .each(new Fields("str"), new CustomerBolt(), new Fields("customerId", "customerContent"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("customerId1234", "customerContent1234"))
                .newValuesStream();
        // .each(new Fields("customers"),
        // new CustomerBolt(), new Fields("customerId", "customerContent"));

        Stream customersContent2 = topology
                .newStream("customersStream2", CustomerKafkaSpout.kafkaSpout())
                //.each(new Fields("customers"), new CustomerBolt(), new Fields("customerId", "customerContent"));
                .each(new Fields("str"), new CustomerBolt(), new Fields("customerId", "customerContent"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("customerId4321", "customerContent4321"))
                .newValuesStream();

        topology.join(customersContent, new Fields("customerId1234"), customersContent2, new Fields("customerId4321"),
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
