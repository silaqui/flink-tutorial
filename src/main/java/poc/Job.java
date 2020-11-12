package poc;

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
import poc.sources.CountSource;
import poc.sources.DataSource;

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

        KeyedStream<CountedDataModel, Integer> countedDataModelIntegerKeyedStream = dataByKey
                .connect(countByKey)
                .process(new ProcessJoin())
                .keyBy(c -> c.data.id)
                .process(new CounterProcessFunction())
                .keyBy(c -> c.data.id)
                .assignTimestampsAndWatermarks(new WatermarkGenerator())
                .keyBy(c -> c.data.id);

        countedDataModelIntegerKeyedStream
                .addSink(new MySink());

        countedDataModelIntegerKeyedStream
                .process(new TriggeredFunction());

        env.execute("Job");
    }

}
