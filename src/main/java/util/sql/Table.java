/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.sql;

import util.NoSQL;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author geomar
 */
public class Table {
    
    private final String name;
    private final NoSQL targetDB ;
    private List<String> pks;
    private List<ForeignKey> fks;
    private List<String> attributes;
    private List<String> keys;

    public String getName() {
        return name;
    }

    public NoSQL getTargetDB() {
        return targetDB;
    }

    public List<String> getPks() {
        return pks;
    }

    public void setPks(List<String> pks) {
        this.pks = pks;
    }

    public List<ForeignKey> getFks() {
        return fks;
    }

    public void setFks(List<ForeignKey> fks) {
        this.fks = fks;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public Table(String name, NoSQL targetDB, List<String> pks, List<ForeignKey> fks, List<String> attributes) {
        this.name = name;
        this.targetDB = targetDB;
        this.pks = pks;
        this.fks = fks;
        this.attributes = attributes;
        this.keys = new ArrayList<>();
    }

    public Table(String name, NoSQL targetDB, List<String> pks, List<ForeignKey> fks, List<String> attributes, List<String> keys) {
        this.name = name;
        this.targetDB = targetDB;
        this.pks = pks;
        this.fks = fks;
        this.attributes = attributes;
        this.keys = keys;
    }  
    
}
