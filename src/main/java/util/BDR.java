/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import util.sql.Table;

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

    public BDR() {
        tables = new ArrayList<>();
    }

    public BDR(String name, List<Table> tables) {
        this.name = name;
        this.tables = tables;
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
}
