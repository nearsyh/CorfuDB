package org.corfudb.runtime.collections;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;

/**
 * List of metrics captured for a stream listener.
 * <p>
 * Created by WenbinZhu on 11/23/20.
 */
public class StreamingMetrics {

    private final String listenerId;

    private final TimingHistogram deliveryDuration;

    StreamingMetrics(StreamListener listener, String namespace, String streamTag, MetricRegistry registry) {
        this.listenerId = String.format("StreamListener_%s_%s_%s", listener, namespace, streamTag);
        this.deliveryDuration = new TimingHistogram(registry.histogram(listenerId + "_deliveryDuration"));
    }

    public void setDeliveryDuration(long elapsedTime) {
        deliveryDuration.update(elapsedTime);
    }

    @Override
    public String toString() {
        JsonObject json = new JsonObject();
        json.addProperty("listenerId", listenerId);
        json.add("deliveryDuration", deliveryDuration.asJsonObject());

        return json.toString();
    }
}
