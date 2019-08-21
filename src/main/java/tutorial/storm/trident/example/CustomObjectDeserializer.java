package tutorial.storm.trident.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CustomObjectDeserializer implements Deserializer<JoinExample.Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public JoinExample.Customer deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        JoinExample.Customer object = null;
        try {
            object = mapper.readValue(data, JoinExample.Customer.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public void close() {
    }
}