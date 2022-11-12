package com.lisa.sqltokeynosql.util;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.util.connectors.*;
import com.lisa.sqltokeynosql.util.connectors.neo4j.Neo4jConnector;

/**
 * @author geomar
 */
public class NoSQL {
    private String alias;
    private String user;
    private String password;
    private String url;
    private final Connector connection;
    private final com.lisa.sqltokeynosql.api.enums.Connector connector;

    public NoSQL(String alias, String user, String password, String url, String conection)
    {
        this.alias = alias;
        this.user = user;
        this.password = password;
        this.url = url;
        this.connector =  com.lisa.sqltokeynosql.api.enums.Connector.valueOf(conection.toUpperCase());
        this.connection = GetConnector(this.connector);
    }

    private Connector GetConnector(com.lisa.sqltokeynosql.api.enums.Connector connector)
    {
        switch (connector)
        {
            case MONGO:
                return new MongoConnector();

            case CASSANDRA2:
                return new Cassandra2Connector();

            case CASSANDRA:
                return new CassandraConnector();

            case REDIS:
                return new RedisConnector();

            case SIMPLE:
                return new SimpleDBConnector();

            case NEO4J:
                return new Neo4jConnector();

            default:
                throw new UnsupportedOperationException("Connector not declared!!!!");
        }
    }

    public String getAlias() {
        return alias;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    public Connector getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return alias;
    }

    public com.lisa.sqltokeynosql.api.enums.Connector getConnector() {
        return connector;
    }

    public void setAlias(String alias)
    {
        this.alias = alias;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }
}
