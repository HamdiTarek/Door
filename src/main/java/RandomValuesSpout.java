import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

class RandomValuesSpout extends BaseRichSpout {




    private final static String[] operations = {
            "1 0 1 0 1 1 1 0 0 0 1 0 1 0 1 0 1 1 0 0 1",
            "0 0 0 1 1 1 0 1 0 1 0 1 0 1 1 1 0 0 0 1 1 1 0 0 1 1 1",
            "0 0 0 1 1 1 0 1 0 1 0 1 0 1 1 1 0 0 0 1 0 1 0 1 0 1 1 0 0 1",
            "1 0 1 1 0 0 1 0 1 0 1 0 1 0 1 1 0 0 1 1 1 1 1 0 0 1 1 1",
            "0 0 1 1 1 1 1 1 0 0 1 1 1 1 1 1 0 0 1 1 1",
            "0 1 0 1 0 1 0 1 1 0 0 1 1 1 1 1 0 0 1 1 1",
            "0 1 1 0 0 1 1 1 0 1 1 1 1 1 0 0 1 1 1",
            "0 1 1 1 1 1 0 0 1 1 1",
            "1 0 1 1 0 0 1 0 1 0 1 0 1 0 1 1 0 0 1 1 1",
            "1 0 1 1 0 0 1 0 1 0 1",
            "0 1 1 0 0 1 1 1 0 1",
            "1 0 1 1 0 0 1 0 1 0 1 0 1",
            "0 0 0 1 1 1 0 1 0 1",
            "0 1 0 1 0 1 0 1 0 1 0 1",
            "0 1 1 0 0 1 1 1 0 1 0 1 0 1",
            "1 0 1 0 1 1 1 0 0 0 1 0",
            "1 0 0 1 1 1 1 0 1 0 1",
            "0 0 1 1 1 1 0 1 0 1 1 0 1 0",
            "0 1 0 1 1 0 1 0 1 0 1 0 0",
            "0 1 0 1 1 0 1 0 1 1 1 0 0",
            "1 0 1 1 1 0 0 0 1 1 1 0 0 0",
            "0 1 1 0 0 1 1 1 0 1 1 1 1 1 0 1 0 1 0 0 1 1 1",
            "0 1 1 1 1 1 0 0 0 0 0 1 1 1 0 0 0",
            "1 0 1 1 0 0 1 0 0 1 1 0 0 0 1 0 1 0 1 0 1 1 0 0 1 1 1",
            "1 0 1 1 0 0 1 0 1 0 1",
            "0 1 1 0 0 1 1 0 0 1 0",
            "1 0 1 1 0 0 1 0 1 0 1 0 1",
            "0 0 0 1 1 1 0 1 0 1",
            "0 1 0 1 0 1 0 1 0 ",
            "0 1 1 0 0 1 1 1 0 1 0 1 0 1",
            "1 0 1 0 1 1 0 0 1 0 0 0 1 0",
            "1 0 0 1 1 1 1 0 1 0 1",
            "0 0 1 1 1 1 0 1 0 0 0 0 1 0 1",
            "0 1 0 1 1 0 0 0 1 0 0 1",
            "0 1 0 1 1 0 1 0 1",
            "1 0 1 1 1 0 0 0",
            "0 1",
            "1 0 1 1 0 0 1"

    };


    private SpoutOutputCollector outputCollector;

    public void open(Map<String, Object> config, TopologyContext context, SpoutOutputCollector collector) {
        outputCollector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("door","operations"));
    }

    public void nextTuple() {
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
            for (int i=0; i<200; i++) {
                outputCollector.emit(new Values("Door "+i, operations[ThreadLocalRandom.current().nextInt(operations.length)]));
            }

}}