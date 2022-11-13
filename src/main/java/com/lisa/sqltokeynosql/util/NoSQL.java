package com.lisa.sqltokeynosql.util;


import com.lisa.sqltokeynosql.api.enums.SgbdConnector;
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
    private final SgbdConnector sgbdConnector;

    public NoSQL(String alias, String user, String password, String url, String conection)
    {
        this.alias = alias;
        this.user = user;
        this.password = password;
        this.url = url;
        this.sgbdConnector = SgbdConnector.valueOf(conection.toUpperCase());
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

    @Override
    public String toString() {
        return alias;
    }

    public SgbdConnector getConnector() {
        return sgbdConnector;
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
