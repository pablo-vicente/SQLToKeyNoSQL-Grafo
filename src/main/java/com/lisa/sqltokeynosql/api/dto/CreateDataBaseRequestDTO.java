package com.lisa.sqltokeynosql.api.dto;

public class CreateDataBaseRequestDTO {
    private String name;
    private String connector;

    public String getName() {
        return name;
    }
    public String getConnector() {
        return connector;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }
}
