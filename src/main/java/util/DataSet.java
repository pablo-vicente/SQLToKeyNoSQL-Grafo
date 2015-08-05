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

    public DataSet() {
        this.columns = new <String> ArrayList();
        this.data = new <String[]> ArrayList();
    }
    
    
}
