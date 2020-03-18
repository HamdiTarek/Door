import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogWriterBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(LogWriterBolt.class);

    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
    }

    public void execute(Tuple input) {
        ConcurrentHashMap<String, Integer> stats_entrances = (ConcurrentHashMap<String, Integer>) input.getValueByField("stats_entrances");
        ConcurrentHashMap<String, Integer> stats_exists = (ConcurrentHashMap<String, Integer>) input.getValueByField("stats_exists");
        ConcurrentHashMap<String, Integer> stats_used = (ConcurrentHashMap<String, Integer>) input.getValueByField("stats_used");

        int max_entrances = Collections.max(stats_entrances.values());
        int min_entrances = Collections.min(stats_entrances.values());
        int max_exists = Collections.max(stats_exists.values());
        int min_exists = Collections.min(stats_exists.values());
        int max_used = Collections.max(stats_used.values());
        int min_used = Collections.min(stats_used.values());

        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : stats_entrances.entrySet()) {
            if (entry.getValue()==max_entrances) {
                keys.add(entry.getKey());
            }
        }
        List<String> keys1 = new ArrayList<>();
        for (Map.Entry<String, Integer> entry1 : stats_entrances.entrySet()) {
            if (entry1.getValue()==min_entrances) {
                keys1.add(entry1.getKey());
            }
        }

        List<String> keys2 = new ArrayList<>();
        for (Map.Entry<String, Integer> entry2 : stats_exists.entrySet()) {
            if (entry2.getValue()==max_exists) {
                keys2.add(entry2.getKey());
            }
        }

        List<String> keys3 = new ArrayList<>();
        for (Map.Entry<String, Integer> entry3 : stats_exists.entrySet()) {
            if (entry3.getValue()==min_exists) {
                keys3.add(entry3.getKey());
            }
        }

        List<String> keys4 = new ArrayList<>();
        for (Map.Entry<String, Integer> entry4 : stats_used.entrySet()) {
            if (entry4.getValue()==max_used) {
                keys4.add(entry4.getKey());
            }
        }

        List<String> keys5 = new ArrayList<>();
        for (Map.Entry<String, Integer> entry5 : stats_used.entrySet()) {
            if (entry5.getValue()==min_used) {
                keys5.add(entry5.getKey());
            }
        }




        logger.info("The '{}' is the most entry door with the value {}", keys.get(0), max_entrances);
        logger.info("The '{}' is the lowest entry door with the value {}", keys1.get(0), min_entrances);

        logger.info("The '{}' is the most exist door with the value {}", keys2.get(0), max_exists);
        logger.info("The '{}' is the lowest exist door with the value {}", keys3.get(0), min_exists);

        logger.info("The '{}' is the most used door with the value {}", keys4.get(0), max_used);
        logger.info("The '{}' is the lowest used door with the value {}", keys5.get(0), min_used);
        System.out.println("------------------------------------------------------------------------------------------------------------");
        System.out.println("////////////////////////////////////////////////////////////////////////////////////////////////////////////");
        System.out.println("////////////////////////////////////////////////////////////////////////////////////////////////////////////");
        System.out.println("------------------------------------------------------------------------------------------------------------");

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}