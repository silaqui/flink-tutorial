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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import poc.model.CountedDataModel;

import java.util.concurrent.ScheduledFuture;

public class MySink extends RichSinkFunction<CountedDataModel> implements CheckpointedFunction, ProcessingTimeService {

    private ListState<CountedDataModel> counterState;
    private ValueState<Integer> currentCount;

    @Override
    public void invoke(CountedDataModel value, Context context) throws Exception {

        System.out.println("Watermark: " + context.currentWatermark());
        System.out.println("Invoke id " + value.data.id);

        if (currentCount.value() == null) {
            currentCount.update(0);
        }
        counterState.add(value);
        currentCount.update(currentCount.value() + 1);

        if (currentCount.value() == 3) {
            System.out.println("MySink Invoked pack of 3");
//            counterState.get().forEach(System.out::println);

            currentCount.update(0);
            counterState.clear();
        }

        if(value.timestamp <= context.currentWatermark()){
            System.out.println("MySink Invoked last pack");
//            counterState.get().forEach(System.out::println);

            currentCount.update(0);
            counterState.clear();

        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        counterState = context
                .getKeyedStateStore()
                .getListState(new ListStateDescriptor<>("counterState", TypeInformation.of(CountedDataModel.class)));
        currentCount = context
                .getKeyedStateStore()
                .getState(new ValueStateDescriptor<>("currentCount", TypeInformation.of(Integer.class)));

    }

    @Override
    public long getCurrentProcessingTime() {
        return 0;
    }

    @Override
    public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
        return null;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
        return null;
    }
}
