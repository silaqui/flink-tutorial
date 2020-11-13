package poc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import poc.fun.CounterProcessFunction;
import poc.fun.MultiplyFun;
import poc.fun.MySink;
import poc.fun.ProcessJoin;
import poc.model.CountModel;
import poc.model.DataModel;
import poc.sources.CountSource;
import poc.sources.DataSource;

@Slf4j
public class Job {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(5);

        KeyedStream<DataModel, Integer> dataByKey = env
                .addSource(new DataSource())
                .name("Data Source")
                .keyBy(d -> d.id);

        KeyedStream<CountModel, Integer> countByKey = env
                .addSource(new CountSource())
                .name("Count Source")
                .keyBy(c -> c.id);

        dataByKey
                .connect(countByKey)

                .process(new ProcessJoin())
                .name("ProcessJoin")
                .keyBy(c -> c.data.id)

                .process(new CounterProcessFunction())
                .name("CounterProcessFunction")
                .keyBy(c -> c.data.id)

                .process(new MultiplyFun())
                .name("MultiplyFun")
                .keyBy(m -> m.id)

                .addSink(new MySink())
                .name("Sink - Parallelism 1")
                .setParallelism(1); //TODO Consider split sink to storage and warehouse store procedure. Then setParallelism 1 only for store procedure.

        env.execute("Job");
    }

}
