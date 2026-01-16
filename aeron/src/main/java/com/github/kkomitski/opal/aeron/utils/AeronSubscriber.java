package com.github.kkomitski.opal.aeron.utils;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

public class AeronSubscriber implements AutoCloseable {
    public static final String UDP_ENDPOINT_PROP = "opal.aeron.udp.sub.endpoint";
    public static final int DEFAULT_PORT = 42069;

    protected final AeronMediaDriver mediaDriver;
    protected final String transport;
    protected final int streamId;
    protected final int port;
    protected final String channel;
    protected final Subscription subscription;

    public AeronSubscriber(final AeronMediaDriver mediaDriver, final String transport, final int streamId) {
        this(mediaDriver, transport, streamId, DEFAULT_PORT);
    }

    public AeronSubscriber(final AeronMediaDriver mediaDriver, final String transport, final int streamId,
            final int port) {
        if (mediaDriver == null) {
            throw new IllegalArgumentException("mediaDriver must not be null");
        }
        if (transport == null || transport.isBlank()) {
            throw new IllegalArgumentException("transport must not be blank");
        }
        this.mediaDriver = mediaDriver;
        this.transport = transport.trim().toLowerCase();
        this.streamId = streamId;
        this.port = port;

        this.channel = resolveChannel();
        this.subscription = mediaDriver.aeron().addSubscription(channel, streamId);
    }

    protected String resolveChannel() {
        if ("ipc".equals(transport)) {
            return "aeron:ipc";
        }

        if ("udp".equals(transport)) {
            return "aeron:udp?endpoint=0.0.0.0:" + Integer.toString(port);
        }

        throw new IllegalArgumentException("Unsupported transport: " + transport);
    }

    public Subscription subscription() {
        return subscription;
    }

    public String channel() {
        return channel;
    }

    public int streamId() {
        return streamId;
    }

    public int poll(final FragmentHandler handler, final int fragmentLimit) {
        return subscription.poll(handler, fragmentLimit);
    }

    @Override
    public void close() {
        subscription.close();
    }
}
