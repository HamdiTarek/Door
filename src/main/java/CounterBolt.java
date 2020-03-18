import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    public void execute(Tuple input) {
        String door = input.getStringByField("door");
        Integer v1 = input.getIntegerByField("v1");
        Integer v2 = input.getIntegerByField("v2");


        outputCollector.emit(new Values(door, v1, v2));
        }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("door", "v1", "v2"));
    }
}