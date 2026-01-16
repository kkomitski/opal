package com.github.kkomitski.opal.aeron.utils;

import io.aeron.Aeron;

/**
 * Small wrapper around an Aeron client connection that attaches to an external Media Driver.
 *
 * Usage:
 * try (ExternalAeron external = ExternalAeron.connect()) {
 *     Aeron aeron = external.aeron();
 *     // pass aeron into publisher/subscriber factories
 * }
 */
public final class AttachAeronMediaDriver implements AutoCloseable {
    public static final String AERON_DIR_PROP = "aeron.dir";
    public static final String DEFAULT_AERON_DIR = System.getProperty("user.dir") + "/shared-memory";

    private final String aeronDir;
    private final Aeron aeron;

    private AttachAeronMediaDriver(final String aeronDir, final Aeron aeron) {
        this.aeronDir = aeronDir;
        this.aeron = aeron;
    }

    /**
     * Connect using {@code -Daeron.dir=...} or default to {@code <user.dir>/shared-memory}.
     */
    public AttachAeronMediaDriver() {
        this(System.getProperty(AERON_DIR_PROP, DEFAULT_AERON_DIR));
    }

    public AttachAeronMediaDriver(final String aeronDir) {
        this(aeronDir, Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir)));
    }

    public static AttachAeronMediaDriver connect() {
        final String aeronDir = System.getProperty(AERON_DIR_PROP, DEFAULT_AERON_DIR);
        return connect(aeronDir);
    }

    public static AttachAeronMediaDriver connect(final String aeronDir) {
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        return new AttachAeronMediaDriver(aeronDir, aeron);
    }

    public String aeronDir() {
        return aeronDir;
    }

    public Aeron aeron() {
        return aeron;
    }

    @Override
    public void close() {
        aeron.close();
    }
}
