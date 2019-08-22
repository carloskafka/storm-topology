package tutorial.storm.trident.example;

import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;

public class CustomerBolt extends BaseFunction {

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