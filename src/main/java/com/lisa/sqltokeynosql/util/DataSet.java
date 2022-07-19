/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author geomar
 */
public class DataSet {
    private List<String> columns;
    private List<String[]> data;
    private String tableName;

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String[]> getData() {
        return data;
    }

    public void setData(List<String[]> data) {
        this.data = data;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    
    
    public DataSet() {
        this.columns = new LinkedList<>();
        this.data = new ArrayList<>();
        tableName = null;
    }
    
    
}
