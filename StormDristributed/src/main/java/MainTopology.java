import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
    public static void main(String[] args)  {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("IntegerSpout",new IntegerSpout());
        topologyBuilder.setBolt("MultiplierBolt",new MultiplierBolt()).shuffleGrouping("IntegerSpout");
        Config config=new Config();
        config.setDebug(true);
//        LocalCluster localCluster=null;
//        LocalCluster  localCluster= new LocalCluster();
        try {

            StormSubmitter.submitTopology("HelloTopology",config,topologyBuilder.createTopology());
//            Thread.sleep(1000);
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
