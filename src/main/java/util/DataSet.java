/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.ArrayList;

/**
 *
 * @author geomar
 */
public class DataSet {
    private ArrayList<String> columns;
    private ArrayList<String[]> data; 
    private String table_n;

    public ArrayList<String> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<String> columns) {
        this.columns = columns;
    }

    public ArrayList<String[]> getData() {
        return data;
    }

    public void setData(ArrayList<String[]> data) {
        this.data = data;
    }

    public String getTable_n() {
        return table_n;
    }

    public void setTable_n(String table_n) {
        this.table_n = table_n;
    }

    
    
    public DataSet() {
        this.columns = new <String> ArrayList();
        this.data = new <String[]> ArrayList();
        table_n = null;
    }
    
    
}
