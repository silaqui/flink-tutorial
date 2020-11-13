package poc.fun;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import poc.model.CountedDataModel;

public class CounterProcessFunction extends KeyedProcessFunction<Integer, CountedDataModel, CountedDataModel> {

    private ValueState<Integer> counterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        counterState = getRuntimeContext().getState(new ValueStateDescriptor<>("counterState", TypeInformation.of(Integer.class)));
    }

    @Override
    public void processElement(CountedDataModel countedDataModel, Context context, Collector<CountedDataModel> collector) throws Exception {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        counterState.update(counterState.value() + 1);

        if (counterState.value() != null && countedDataModel.count.count == counterState.value()) {
            countedDataModel.setLast(true);
            System.out.println("CounterProcessFunction " + countedDataModel);
            counterState.clear();
        }
        collector.collect(countedDataModel);

    }

}
