package com.bol.team77;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import com.bol.team77.domain.Key;
import com.bol.team77.domain.MessageWithSomeEnum;
import com.bol.team77.domain.SomeEnum;

public class TestAvroEnum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new Generator()).setParallelism(32)
           .keyBy(selectAvroKey_does_work())
           .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
           .trigger(CountTrigger.of(3200000))
           .aggregate(new Counter()).setParallelism(32)
           .print();

        env.setParallelism(32);
        env.setMaxParallelism(32);
        env.execute("hi");
    }

    private static KeySelector<MessageWithSomeEnum, SomeEnum> selectEnum_does_not_work() {
        return message -> message.getKey().getSomeEnum();
    }

    private static KeySelector<MessageWithSomeEnum, Key> selectAvroKey_does_work() {
        return message -> message.getKey();
    }
}