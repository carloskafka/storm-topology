package tutorial.storm.trident.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import scala.actors.threadpool.Arrays;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

public class CustomerFileRepository {

    private static int generateRandom(List randomObjects) {
        int min = 0;
        int max = randomObjects.size() - 1;

        int currentValue = new Random().nextInt((max - min) + 1) + min;

        return currentValue;
    }

    public static void initializeCustomersFile() {
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
}
