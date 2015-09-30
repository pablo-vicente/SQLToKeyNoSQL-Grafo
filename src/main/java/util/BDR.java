/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.io.Serializable;
import java.util.ArrayList;

/**
 *
 * @author geomar
 */
public class BDR implements Serializable{
    private String name;
    private ArrayList<Table> tables;

    public BDR() {
        tables = new ArrayList<>();
    }

    public BDR(String name, ArrayList<Table> tables) {
        this.name = name;
        this.tables = tables;
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<Table> getTables() {
        return tables;
    }
    
     public Table getTable(String t) {
         for (Table table: tables){
             if (t.equals(table.getName()))
                 return table;
         }
        return null;
    }

    public void setTables(ArrayList<Table> tables) {
        this.tables = tables;
    }

    @Override
    public String toString() {
        return name;
    }
    
    
    
}
