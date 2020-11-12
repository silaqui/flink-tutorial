package tutorial;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FiltersStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> dataStream = env.socketTextStream("localhost", 9999).filter(new Filter());

        dataStream.print();

        env.execute("FilterStreams Strings");

    }

    public static class Filter implements FilterFunction<String> {

        public boolean filter(String s) throws Exception {
            try {

                Double.parseDouble(s);
                return true;
            } catch (Exception ignored) {}
            return false;
        }
    }

}
