package com.bol.team77;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import com.bol.team77.domain.MessageWithSomeEnum;
import com.bol.team77.domain.SomeEnum;

public class Counter implements AggregateFunction<MessageWithSomeEnum, Tuple2<SomeEnum, Integer>, Tuple2<SomeEnum, Integer>> {
    @Override
    public Tuple2<SomeEnum, Integer> createAccumulator() {
        return Tuple2.of(null, 0);
    }

    @Override
    public Tuple2<SomeEnum, Integer> add(MessageWithSomeEnum value, Tuple2<SomeEnum, Integer> accumulator) {
        return Tuple2.of(value.getKey().getSomeEnum(), accumulator.f1 + 1);
    }

    @Override
    public Tuple2<SomeEnum, Integer> getResult(Tuple2<SomeEnum, Integer> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<SomeEnum, Integer> merge(Tuple2<SomeEnum, Integer> a, Tuple2<SomeEnum, Integer> b) {
        return Tuple2.of(a.f0, a.f1 + b.f1);
    }
}