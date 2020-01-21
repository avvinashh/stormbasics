import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class FileSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;
    private boolean finished;

    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        String filePath = conf.get("filePath").toString();
        try {
            bufferedReader = Files.newBufferedReader(Paths.get(filePath));
        } catch (IOException e) {
            throw new RuntimeException("Error reading the file");
        }

    }

    public void nextTuple() {
        if (!finished) {
            try {
                Thread.sleep(10);
                String keyword = bufferedReader.readLine();
                if (StringUtils.isNotEmpty(keyword)) {
                    this.spoutOutputCollector.emit(new Values(keyword));
                } else {
                    finished = true;
                    bufferedReader.close();
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
