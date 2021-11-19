package com.lisa.sqltokeynosql.api.dto;

import com.lisa.sqltokeynosql.api.enums.Connector;

public class NoSqlTargetDTO {
    private String url;
    private String user;
    private String password;
    private String name;
    private Connector connector;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }
}
