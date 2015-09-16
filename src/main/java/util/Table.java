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
public class Table {
    
    private String name;
    private NoSQL targetDB ;
    private ArrayList<String> pks;
    private ArrayList<String> fks;
    private ArrayList<String> attributes;
    private ArrayList<String> keys;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public NoSQL getTargetDB() {
        return targetDB;
    }

    public void setTargetDB(NoSQL targetDB) {
        this.targetDB = targetDB;
    }

    public ArrayList<String> getPks() {
        return pks;
    }

    public void setPks(ArrayList<String> pks) {
        this.pks = pks;
    }

    public ArrayList<String> getFks() {
        return fks;
    }

    public void setFks(ArrayList<String> fks) {
        this.fks = fks;
    }

    public ArrayList<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(ArrayList<String> attributes) {
        this.attributes = attributes;
    }

    public ArrayList<String> getKeys() {
        return keys;
    }

    public void setKeys(ArrayList<String> keys) {
        this.keys = keys;
    }

    public Table(String name, NoSQL targetDB, ArrayList<String> pks, ArrayList<String> fks, ArrayList<String> attributes) {
        this.name = name;
        this.targetDB = targetDB;
        this.pks = pks;
        this.fks = fks;
        this.attributes = attributes;
        this.keys = new ArrayList<String>();
    }

    public Table(String name, NoSQL targetDB, ArrayList<String> pks, ArrayList<String> fks, ArrayList<String> attributes, ArrayList<String> keys) {
        this.name = name;
        this.targetDB = targetDB;
        this.pks = pks;
        this.fks = fks;
        this.attributes = attributes;
        this.keys = keys;
    }  
    
}
