package com.lisa.sqltokeynosql.util;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.util.connectors.MongoConnector;

/**
 * @author geomar
 */
public class NoSQL {
    private String alias;
    private String user;
    private String password;
    private String url;
    private Connector connection;

    public NoSQL(String alias, String user, String password, String url, Connector conection) {
        this.alias = alias;
        this.user = user;
        this.password = password;
        this.url = url;
        this.connection = conection;
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

    public void setConnection(Connector connection) {
        this.connection = connection;
    }

    @Override
    public String toString() {
        return alias;
    }
}
