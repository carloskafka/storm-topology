package tutorial.storm.trident.example;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class CustomerStoreSpout extends BaseRichSpout {
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