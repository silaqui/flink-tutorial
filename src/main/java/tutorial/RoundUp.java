package tutorial;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RoundUp {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Long> dataStream = env
                .socketTextStream("localhost", 9999)
                .filter(new FiltersStream.Filter())
                .map(new Round());

        dataStream.print();

        env.execute("FilterStreams Strings");

    }

    public static class Round implements MapFunction<String, Long> {

        public Long map(String s) throws Exception {

            double v = Double.parseDouble(s);
            return Math.round(v);
        }
    }
}
