package poc.fun;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import poc.model.MultiplyDataModel;

public class MySink extends RichSinkFunction<MultiplyDataModel> implements CheckpointedFunction {

    private ListState<MultiplyDataModel> counterState;
    private ValueState<Integer> currentCount;
    private ValueState<Boolean> lastValueReceived;

    @Override
    public void invoke(MultiplyDataModel value, Context context) throws Exception {
        System.out.println("Invoke id " + value.id);
        if (currentCount.value() == null) {
            currentCount.update(0);
        }
        counterState.add(value);
        currentCount.update(currentCount.value() + 1);

        if (currentCount.value() == 3) {
            System.out.println("MySink Invoked pack of 3");
            currentCount.update(0);
            counterState.clear();
        }
        if (value.isLast) {
            System.out.println("Trigger store procedure");
            counterState.clear();
            currentCount.clear();
            lastValueReceived.clear();
        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        counterState = context
                .getKeyedStateStore()
                .getListState(new ListStateDescriptor<>("counterState", TypeInformation.of(MultiplyDataModel.class)));
        currentCount = context
                .getKeyedStateStore()
                .getState(new ValueStateDescriptor<>("currentCount", TypeInformation.of(Integer.class)));
        lastValueReceived = context
                .getKeyedStateStore()
                .getState(new ValueStateDescriptor<>("lastValueReceived", TypeInformation.of(Boolean.class)));

    }

}
