package com.lisa.sqltokeynosql.api.enums;

import com.lisa.sqltokeynosql.util.connectors.*;

public enum Connector {
    MONGO(new MongoConnector()),
    REDIS(new RedisConnector()),
    CASSANDRA(new CassandraConnector()),
    CASSANDRA2(new Cassandra2Connector()),
    SIMPLE(new SimpleDBConnector());

    private final com.lisa.sqltokeynosql.architecture.Connector connector;

    Connector(com.lisa.sqltokeynosql.architecture.Connector connector) {
        this.connector = connector;
    }

    public com.lisa.sqltokeynosql.architecture.Connector getValue() {
        return connector;
    }
}