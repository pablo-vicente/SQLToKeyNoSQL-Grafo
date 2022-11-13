package com.lisa.sqltokeynosql.api.enums;

public enum SgbdConnector {
    MONGO (1),
    REDIS(2),
    CASSANDRA(3),
    CASSANDRA2(4),
    SIMPLE(5),
    NEO4J(6);

    private int id; // Could be other data type besides int
     SgbdConnector(int id) {
        this.id = id;
    }

    private int getId() {
        return this.id;
    }

    public static SgbdConnector fromId(int id) {
        for (SgbdConnector type : values()) {
            if (type.getId() == id) {
                return type;
            }
        }
        return null;
    }
}