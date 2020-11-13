package poc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import poc.fun.CounterProcessFunction;
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

        SingleOutputStreamOperator<DataModel> dataSource = env.addSource(new DataSource()).name("Data Source");
        SingleOutputStreamOperator<CountModel> countSource = env.addSource(new CountSource()).name("Count Source");

        KeyedStream<DataModel, Integer> dataByKey =
                dataSource.keyBy((KeySelector<DataModel, Integer>) dataModel -> dataModel.id);
        KeyedStream<CountModel, Integer> countByKey =
                countSource.keyBy((KeySelector<CountModel, Integer>) countModel -> countModel.id);

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
                .keyBy(c -> c.id)
                .addSink(new MySink())
                .name("Sink ...")
                .setParallelism(1); //TODO Consider split sink to storage and warehouse store procedure. Then setParallelism 1 only for store procedure.

        env.execute("Job");
    }

}
