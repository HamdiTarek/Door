import java.util.Arrays;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitValues extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String door = input.getStringByField("door");
        String operations = input.getStringByField("operations");
        int v1=0;
        int v2=0;
        char[] data = new char[operations.length()];

        for (int i=0;i<operations.length();i++) {
            data[i] = operations.charAt(i);
        }
        for (char c : data) {
            if (c=='1'){
                v1++;
            } else if (c=='0') {
                v2++;
            }
        }

        collector.emit(new Values(door, v1, v2));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("door", "v1", "v2"));
    }
}