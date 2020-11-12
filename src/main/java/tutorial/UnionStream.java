package tutorial;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionStream {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 8000);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9000);

        if(stream1 == null || stream2 == null){
            System.exit(1);
            return;
        }

        DataStream<String> union = stream1.union(stream2);

        union.print();

        env.execute("Union");
    }
}
