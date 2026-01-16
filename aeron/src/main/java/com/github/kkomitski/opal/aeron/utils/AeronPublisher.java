package com.github.kkomitski.opal.aeron.utils;

import org.agrona.DirectBuffer;

import io.aeron.Publication;

public class AeronPublisher implements AutoCloseable {
    public static final String UDP_ENDPOINT_PROP = "opal.aeron.udp.pub.endpoint";

    protected final AeronMediaDriver mediaDriver;
    protected final String transport;
    protected final int streamId;
    protected final String channel;
    protected final Publication publication;

    public AeronPublisher(final AeronMediaDriver mediaDriver, final String transport, final int streamId) {
        if (mediaDriver == null)
        {
            throw new IllegalArgumentException("mediaDriver must not be null");
        }
        if (transport == null || transport.isBlank())
        {
            throw new IllegalArgumentException("transport must not be blank");
        }
        this.mediaDriver = mediaDriver;
        this.transport = transport.trim().toLowerCase();
        this.streamId = streamId;

        this.channel = resolveChannel();
        this.publication = mediaDriver.aeron().addPublication(channel, streamId);
    }

    protected String resolveChannel() {
        if ("ipc".equals(transport)) {
            return "aeron:ipc";
        }

        if ("udp".equals(transport)) {
            return "aeron:udp?endpoint=" + udpEndpoint();
        }

        throw new IllegalArgumentException("Unsupported transport: " + transport);
    }

    /**
     * Override in subclasses for fixed endpoints.
     * Defaults to system property {@code opal.aeron.udp.pub.endpoint}.
     */
    protected String udpEndpoint() {
        final String endpoint = System.getProperty(UDP_ENDPOINT_PROP);
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException(
                "UDP endpoint not configured. Either override udpEndpoint() or set -D" + UDP_ENDPOINT_PROP + "=host:port");
        }
        return endpoint;
    }

    public Publication publication() {
        return publication;
    }

    public String channel() {
        return channel;
    }

    public int streamId() {
        return streamId;
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length) {
        return publication.offer(buffer, offset, length);
    }

    @Override
    public void close() {
        publication.close();
    }
}
