package me.oleniuk.learn.kafkastreamstest;


import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class TimestampOrderingProcessor extends AbstractProcessor<String, String> {

    private long lastTimestamp = Long.MIN_VALUE;

    @Override
    public void process(String key, String value) {
        long currentTimestamp = context().timestamp();
        if (currentTimestamp < lastTimestamp) {
            throw new IllegalStateException("Received events are not in order.");
        }
        lastTimestamp = currentTimestamp;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
    }
}

