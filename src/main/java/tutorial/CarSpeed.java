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
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

public class CarSpeed {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        DataStreamSource<String> streamSource = env.addSource(new TwitterSource(params.getProperties()));

        DataStream<String> speed = streamSource
                .map(new Speed())
                .keyBy(0)
                .flatMap(new AverageReduceListState()); //AverageSpeedValueState //AverageSpeedListState //AverageReduceListState

        speed.print();

        env.execute("Speed");
    }

    public static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {
        public Tuple2<Integer, Double> map(String s) throws Exception {
            return Tuple2.of(1, Double.parseDouble(s));
        }
    }

    public static class AverageSpeedValueState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

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

    public static class AverageSpeedListState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ListState<Double> speedListState;

        public void flatMap(Tuple2<Integer, Double> input, Collector<String> collector) throws Exception {
            if (input.f1 >= 65) {

                Iterable<Double> carSpeeds = speedListState.get();

                int count = 0;
                int sum = 0;

                for (Double carSpeed : carSpeeds
                ) {
                    count++;
                    sum += carSpeed;
                }

                String.format("EXCEEDED ! The Average speed of last %s car(s) was %s, your speed is %s",
                        count,
                        sum / count,
                        input.f1
                );

            } else {
                collector.collect("Thank you for staying under limit!");
            }
            speedListState.add(input.f1);
        }

        public void open(Configuration configuration) {
            ListStateDescriptor<Double> descriptor = new ListStateDescriptor<Double>("carAverageSpeed", Double.class);
            speedListState = getRuntimeContext().getListState(descriptor);
        }


    }

    public static class AverageReduceListState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ValueState<Integer> countState;
        private transient ReducingState<Double> sumState;

        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out) throws Exception {
            if (input.f1 >= 65) {
                Double sumSpeed = sumState.get();
                Integer count = countState.value();


                String.format("EXCEEDED ! The Average speed of last %s car(s) was %s, your speed is %s",
                        count,
                        sumSpeed / count,
                        input.f1
                );

                countState.clear();
                sumState.clear();
            } else {
                out.collect("Thank you for staying under limit!");
            }

            countState.update(countState.value() + 1);
            sumState.add(input.f1);
        }

        public void open(Configuration configuration) {
            ValueStateDescriptor<Integer> valueStateDescriptor =
                    new ValueStateDescriptor<Integer>(
                            "carCount",
                            Integer.class,
                            0);
            countState = getRuntimeContext().getState(valueStateDescriptor);

            ReducingStateDescriptor<Double> averageSpeed = new ReducingStateDescriptor<Double>(
                    "averageSpeed",
                    new ReduceFunction<Double>() {
                        public Double reduce(Double cumulative, Double input) throws Exception {
                            return cumulative + input;
                        }
                    }, Double.class);

            sumState = getRuntimeContext().getReducingState(averageSpeed);
        }


    }
}
