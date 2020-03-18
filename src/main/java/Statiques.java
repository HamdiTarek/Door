import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Statiques extends BaseRichBolt {
    private OutputCollector outputCollector;
    ConcurrentHashMap<String, Integer> stats_entrances = new ConcurrentHashMap<String, Integer>();
    ConcurrentHashMap<String, Integer> stats_exists = new ConcurrentHashMap<String, Integer>();
    ConcurrentHashMap<String, Integer> stats_used = new ConcurrentHashMap<String, Integer>();

    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;

    }

    public void execute(Tuple input) {

        String door = input.getStringByField("door");
        Integer v1 = input.getIntegerByField("v1");
        Integer v2 = input.getIntegerByField("v2");
        stats_entrances.put(door,v1);
        stats_exists.put(door,v2);
        stats_used.put(door,v1+v2);


        outputCollector.emit(new Values(stats_entrances, stats_exists, stats_used));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stats_entrances", "stats_exists", "stats_used"));
    }
}