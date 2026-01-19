package com.github.kkomitski.opal.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public final class OpalConfig {
    public static final String MARKETS_XML_URL;
    public static final int MATCHER_INGRESS_PORT;
    public static final int MATCHER_INGRESS_STREAM_ID;
    public static final int MATCHER_EGRESS_STREAM_ID;
    public static final int PROMETHEUS_PORT;
    public static final int AERON_FRAGMENT_LIMIT;

    // CPU Core Affinity
    public static final int OS_CORE;
    public static final int GC_CORE;
    public static final int AERON_CONDUCTOR_CORE;
    public static final int AERON_SENDER_CORE;
    public static final int AERON_RECEIVER_CORE;
    public static final int MESSAGING_SERVICE_CORE;
    public static final int MATCHER_SHARD_1_CORE;
    public static final int MATCHER_SHARD_2_CORE;
    public static final int MATCHER_SHARD_3_CORE;
    public static final int MATCHER_SHARD_4_CORE;
    public static final int MATCHER_SHARD_5_CORE;

    static {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }

        MARKETS_XML_URL = require(props, "MARKETS_XML_URL");
        MATCHER_INGRESS_PORT = Integer.parseInt(require(props, "MATCHER_INGRESS_PORT"));
        MATCHER_INGRESS_STREAM_ID = Integer.parseInt(require(props, "MATCHER_INGRESS_STREAM_ID"));
        MATCHER_EGRESS_STREAM_ID = Integer.parseInt(require(props, "MATCHER_EGRESS_STREAM_ID"));
        PROMETHEUS_PORT = Integer.parseInt(require(props, "PROMETHEUS_PORT"));
        AERON_FRAGMENT_LIMIT = Integer.parseInt(require(props, "AERON_FRAGMENT_LIMIT"));

        // CPU Core Affinity
        OS_CORE = Integer.parseInt(require(props, "OS_CORE"));
        GC_CORE = Integer.parseInt(require(props, "GC_CORE"));
        AERON_CONDUCTOR_CORE = Integer.parseInt(require(props, "AERON_CONDUCTOR_CORE"));
        AERON_SENDER_CORE = Integer.parseInt(require(props, "AERON_SENDER_CORE"));
        AERON_RECEIVER_CORE = Integer.parseInt(require(props, "AERON_RECEIVER_CORE"));
        MESSAGING_SERVICE_CORE = Integer.parseInt(require(props, "MESSAGING_SERVICE_CORE"));
        MATCHER_SHARD_1_CORE = Integer.parseInt(require(props, "MATCHER_SHARD_1_CORE"));
        MATCHER_SHARD_2_CORE = Integer.parseInt(require(props, "MATCHER_SHARD_2_CORE"));
        MATCHER_SHARD_3_CORE = Integer.parseInt(require(props, "MATCHER_SHARD_3_CORE"));
        MATCHER_SHARD_4_CORE = Integer.parseInt(require(props, "MATCHER_SHARD_4_CORE"));
        MATCHER_SHARD_5_CORE = Integer.parseInt(require(props, "MATCHER_SHARD_5_CORE"));
    }

    private OpalConfig() {}

    private static String require(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new RuntimeException("Missing required config property: " + key);
        }
        return value;
    }
}