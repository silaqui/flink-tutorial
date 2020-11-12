package tutorial;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Words {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream;

        if (params.has("Input")) {
            System.out.println("Executing Words Example with file input");
            dataStream = env.readTextFile(params.get("input"));
        } else if (params.has("host") && params.has("port")) {
            System.out.println("Executing Words example with socket stream");
            dataStream = env.socketTextStream(
                    params.get("host"),
                    Integer.parseInt(params.get("port"))
            );
        } else {
            System.out.println("Use --host and --port to specify socket");
            System.out.println("Use --input to specify file input");
            System.exit(1);
            return;
        }

        DataStream<String> wordDataStream = dataStream.flatMap(new Splitter());

        wordDataStream.print();

        env.execute();

    }

    private static class Splitter implements FlatMapFunction<String, String> {
        public void flatMap(String sentence, Collector<String> collector) throws Exception {
            for (String word : sentence.split(" ")
            ) {
                collector.collect(word);
            }
        }
    }
}
