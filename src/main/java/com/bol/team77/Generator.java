package com.bol.team77;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.bol.team77.domain.Key;
import com.bol.team77.domain.MessageWithSomeEnum;
import com.bol.team77.domain.SomeEnum;

public class Generator implements SourceFunction<MessageWithSomeEnum>, ParallelSourceFunction<MessageWithSomeEnum> {
    @Override
    public void run(SourceContext<MessageWithSomeEnum> sourceContext) throws Exception {
        Arrays.stream(SomeEnum.values())
              .flatMap(this::generate)
              .forEach(sourceContext::collect);
    }

    private Stream<MessageWithSomeEnum> generate(SomeEnum someEnum) {
        return LongStream.rangeClosed(1, 100000)
                         .mapToObj(id -> new MessageWithSomeEnum(id, new Key(someEnum)));
    }

    @Override
    public void cancel() {

    }
}