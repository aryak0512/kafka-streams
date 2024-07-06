package com.aryak.kafka_stream.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class ProcessHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamThreadExceptionResponse handle(Throwable throwable) {
        log.info("Exception occurred in stream processing : {}", throwable.getMessage(), throwable);
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }
}
