package tutorial.storm.trident.example;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class PrintCustomer extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.out.println("CustomerID: " + tuple.getValueByField("customerId").toString());
        //System.out.println("Customer: " + tuple.getValueByField("customerContent").toString());
        return true;
    }
}