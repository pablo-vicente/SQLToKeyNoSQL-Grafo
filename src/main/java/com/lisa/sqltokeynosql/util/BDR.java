/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.util;

import com.lisa.sqltokeynosql.util.sql.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author geomar
 */
public class BDR implements Serializable {
    private String name;
    private List<Table> tables;
    private final NoSQL targetDB ;

    public BDR(String name, NoSQL targetDB, List<Table> tables)
    {
        this.name = name;
        this.tables = tables;
        this.targetDB = targetDB;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Table> getTables() {
        return tables;
    }

    public Optional<Table> getTable(String tableName) {
        return tables.stream()
                .filter(table -> table.getName().equals(tableName))
                .findFirst();
    }

    @Override
    public String toString() {
        return name;
    }

    public NoSQL getTargetDB()
    {
        return targetDB;
    }
}
