import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FileSpout", new FileSpout());
        builder.setSpout("SignalSpout", new SignalSpout());
        builder.setBolt("WordCountBolt", new WordCountBolt(),3).fieldsGrouping("FileSpout",new Fields("word"));
        builder.setBolt("ReportBolt", new ReportBolt()).globalGrouping("WordCountBolt").shuffleGrouping("SignalSpout","signalstream");
        Config config = new Config();
        config.put("filePath", "C:\\Users\\AvvinAsh\\Documents\\Java Programs\\StormBasics\\WordTopology\\src\\main\\resources\\KeyWords.txt");
        LocalCluster localCluster = new LocalCluster();


        try {
            localCluster.submitTopology("WordCountTopology",config,builder.createTopology());
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            localCluster.shutdown();
        }

    }
}
