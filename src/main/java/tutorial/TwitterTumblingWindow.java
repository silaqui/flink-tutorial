package tutorial;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

public class TwitterTumblingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        DataStreamSource<String> streamSource = env.addSource(new TwitterSource(params.getProperties()));

        DataStream<Tuple2<String, Integer>> tweets = streamSource.flatMap(new LanguageCount())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        tweets.print();

        env.execute("Twitter language");
    }


    public static class LanguageCount implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private transient ObjectMapper jsonParser;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {

            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            String language = jsonNode.has("user") && jsonNode.get("user").has("lang") ?
                    jsonNode.get("user").get("lang").textValue() : "unknown";

            collector.collect(new Tuple2<String, Integer>(language, 1));

        }
    }

}
