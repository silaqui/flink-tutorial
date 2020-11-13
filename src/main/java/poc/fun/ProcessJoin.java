package poc.fun;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import poc.model.CountModel;
import poc.model.CountedDataModel;
import poc.model.DataModel;


public class ProcessJoin extends KeyedCoProcessFunction<Integer, DataModel, CountModel, CountedDataModel> {

    private ListState<DataModel> dataState;
    private ValueState<CountModel> countState; // TODO How to clean this state after job.

    @Override
    public void open(Configuration parameters) throws Exception {
        dataState = getRuntimeContext().getListState(new ListStateDescriptor<>("DataState", TypeInformation.of(DataModel.class)));
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("CountState", TypeInformation.of(CountModel.class)));
    }

    @Override
    public void processElement1(DataModel dataModel, Context context, Collector<CountedDataModel> collector) throws Exception {
        dataState.add(dataModel);
        processJoin(collector);
    }

    @Override
    public void processElement2(CountModel countModel, Context context, Collector<CountedDataModel> collector) throws Exception {
        countState.update(countModel);
        processJoin(collector);
    }

    private void processJoin(Collector<CountedDataModel> collector) throws Exception {
        if (countState.value() != null) {
            for (DataModel dataModel : dataState.get()
            ) {
                collector.collect(new CountedDataModel(dataModel, countState.value(), false, System.currentTimeMillis()));
            }
            dataState.clear();
        }
    }
}