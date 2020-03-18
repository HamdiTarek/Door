import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;

public class Main {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        RandomValuesSpout sentenceGenerator = new RandomValuesSpout();
        SplitValues splitter = new SplitValues();
        CounterBolt counter = new CounterBolt();
        Statiques statiques = new Statiques();
        LogWriterBolt logWriter = new LogWriterBolt();

        builder.setSpout("sentenceGenerator", sentenceGenerator);
        builder.setBolt("splitter", splitter).shuffleGrouping("sentenceGenerator");
        builder.setBolt("counter", counter).shuffleGrouping("splitter");
        builder.setBolt("statiques", statiques).shuffleGrouping("counter");
        builder.setBolt("logWriter", logWriter, 2).globalGrouping("statiques");

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mainTopology", config, builder.createTopology());
        TimeUnit.SECONDS.wait(10);
        cluster.killTopology("mainTopology");
        cluster.shutdown();
    }
}