package tutorial;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountCountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        SingleOutputStreamOperator<WordCount> sum = dataStream.flatMap(new WordCountSplitter())
                .keyBy("word")
                .countWindow(3)
                .sum("count");

        sum.print();

        env.execute("tumbling and Sliding Window");
    }

    public static class WordCountSplitter implements FlatMapFunction<String, WordCount> {

        public void flatMap(String s, Collector<WordCount> collector) throws Exception {
            for (String word : s.split(" ")
            ) {
                collector.collect(new WordCount(word, 1));
            }
        }
    }

    public static class WordCount {
        public String word;
        public Integer count;

        public WordCount() {
        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }
    }


}
