import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ReportBolt extends BaseBasicBolt {

    private HashMap<String, Integer> counterMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        counterMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (!tuple.getSourceStreamId().equals("signalstream")) {
            String word = tuple.getString(0);
            Integer count = tuple.getInteger(1);
            counterMap.put(word, count);
        } else {
            System.out.println("<----Most hit searches---->");
            counterMap.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).limit(5).forEach(System.out::println);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
