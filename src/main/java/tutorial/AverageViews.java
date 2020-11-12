package tutorial;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageViews {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        SingleOutputStreamOperator<Tuple2<String, Double>> stream =
                dataStream.map(new RowSplitter())
                        .keyBy(0)
                        .reduce(new SumAndCount())
                        .map(new Average());

        stream.print();

        env.execute("Word Count");
    }

    public static class RowSplitter implements
            MapFunction<String, Tuple3<String, Double, Integer>> {

        public Tuple3<String, Double, Integer> map(String s) throws Exception {
            String[] fields = s.split(" ");
            if (fields.length == 2) {
                return new Tuple3<String, Double, Integer>(
                        fields[0],
                        Double.parseDouble(fields[1]),
                        1
                );
            }
            return null;
        }
    }

    public static class SumAndCount implements
            ReduceFunction<Tuple3<String, Double, Integer>> {

        public Tuple3<String, Double, Integer> reduce(
                Tuple3<String, Double, Integer> cumulative,
                Tuple3<String, Double, Integer> input) throws Exception {
            return new Tuple3<String, Double, Integer>(
                    input.f0,
                    cumulative.f1 + input.f1,
                    cumulative.f2 + input.f2
            );
        }
    }

    public static class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) throws Exception {
            return new Tuple2<String, Double>(
                    input.f0,
                    input.f1 / input.f2
            );
        }
    }
}
