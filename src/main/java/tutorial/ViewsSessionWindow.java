package tutorial;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ViewsSessionWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        SingleOutputStreamOperator<Tuple3<String, String, Double>> max = dataStream.map(new RowSplitter())
                .keyBy(0, 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .max(2);

        max.print();

        env.execute("tumbling and Sliding Window");
    }

    public static class RowSplitter implements MapFunction<String, Tuple3<String, String, Double>> {

        public Tuple3<String, String, Double> map(String s) throws Exception {
            String[] fields = s.split(",");
            if (fields.length == 3)
                return new Tuple3(
                        fields[0],
                        fields[1],
                        Double.parseDouble(fields[3])
                );

            return null;
        }
    }
}

