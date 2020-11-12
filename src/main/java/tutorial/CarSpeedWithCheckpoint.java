package tutorial;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

public class CarSpeedWithCheckpoint {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        DataStreamSource<String> streamSource = env.addSource(new TwitterSource(params.getProperties()));

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new FsStateBackend("file://tmp/flink/checkpoints"));

        DataStream<String> speed = streamSource
                .map(new Speed())
                .keyBy(0)
                .flatMap(new AverageSpeed());

        speed.print();

        env.execute("Speed");
    }

    public static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {
        public Tuple2<Integer, Double> map(String s) {
            try{
            return Tuple2.of(1, Double.parseDouble(s));

            }catch (Exception e){
                System.out.println(e);
            }
        return null;
        }

    }

    public static class AverageSpeed extends RichFlatMapFunction<Tuple2<Integer, Double>,String> {

        private transient ValueState<Tuple2<Integer, Double>> countSumState;

        public void flatMap(Tuple2<Integer, Double> input, Collector<String> collector) throws Exception {

            Tuple2<Integer, Double> currentCountSumState = this.countSumState.value();

            if (input.f1 >= 65) {
                collector.collect(
                        String.format("EXCEEDED ! The Average speed of last %s car(s) was %s, your speed is %s",
                                currentCountSumState.f0,
                                currentCountSumState.f1 / currentCountSumState.f0,
                                input.f1
                        )
                );
                countSumState.clear();
                currentCountSumState = countSumState.value();
            } else {
                collector.collect("Thank you for staying under limit!");
            }
            currentCountSumState.f0 += 1;
            currentCountSumState.f1 += input.f1;
            countSumState.update(currentCountSumState);
        }

        public void open(Configuration configuration) {
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor =
                    new ValueStateDescriptor<Tuple2<Integer, Double>>("carAverageSpeed",
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
                            }),
                            Tuple2.of(0, 0.0));

            countSumState = getRuntimeContext().getState(descriptor);
        }
    }

}
