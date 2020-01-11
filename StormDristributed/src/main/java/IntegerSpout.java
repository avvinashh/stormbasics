import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static java.lang.Thread.*;

public class IntegerSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    private Integer index=0;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }

    public void nextTuple() {
        if(index<10000)
        {
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.spoutOutputCollector.emit(new Values(index));
            index++;
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("OutputField"));
    }
}
