package poc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import poc.fun.*;
import poc.model.CountModel;
import poc.model.CountedDataModel;
import poc.model.DataModel;
import poc.model.MultiplyDataModel;
import poc.sources.CountSource;
import poc.sources.DataSource;

@Slf4j
public class Job {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000L);

        DataStreamSource<DataModel> dataSource = env.addSource(new DataSource());
        DataStreamSource<CountModel> countSource = env.addSource(new CountSource());

        KeyedStream<DataModel, Integer> dataByKey =
                dataSource.keyBy((KeySelector<DataModel, Integer>) dataModel -> dataModel.id);
        KeyedStream<CountModel, Integer> countByKey =
                countSource.keyBy((KeySelector<CountModel, Integer>) countModel -> countModel.id);

        dataByKey
                .connect(countByKey)
                .process(new ProcessJoin())
                .keyBy(c -> c.data.id)
                .process(new CounterProcessFunction())
                .keyBy(c -> c.data.id)
                .process(new MultiplyFun())
                .keyBy(c -> c.id)
                .addSink(new MySink())
                .setParallelism(1); //TODO Consider split sink to storage and warehouse store procedure. Then setParallelism 1 only for store procedure.

        env.execute("Job");
    }

}
