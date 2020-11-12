//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.util.Collector;
//
//public class WordCount {
//
//    public static void main(String[] args) throws Exception {
//
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
////        SingleOutputStreamOperator<Tuple2<String, Integer>> sum =
////                dataStream.flatMap(new WordCountSplitter())
////                        .keyBy(0)
////                        .sum(1);
////
////        sum.print();
//
//        env.execute("Word Count");
//    }
//
//    public static class WordCountSplitter implements SourceFunction<poc.MyModel> {
//
//
//        public void run(SourceContext<poc.MyModel> sourceContext) throws Exception {
//
//        }
//
//        public void cancel() {
//
//        }
//    }
//
//
//}
