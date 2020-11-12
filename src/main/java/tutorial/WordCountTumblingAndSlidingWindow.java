//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//public class WordCountTumblingAndSlidingWindow {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        ParameterTool params = ParameterTool.fromArgs(args);
//
//        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
//
//        if (dataStream == null) {
//            System.exit(1);
//            return;
//        }
//
//        dataStream.flatMap(new WordCount.WordCountSplitter())
//                .keyBy(0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(10)))
//                .sum(1);
//
//
//        env.execute("tumbling and Sliding Window");
//    }
//
//}
