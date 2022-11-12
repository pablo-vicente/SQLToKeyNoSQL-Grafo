/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.util.sql;

import com.lisa.sqltokeynosql.util.NoSQL;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author geomar
 */
public class Table {
    
    private final String name;
    private List<String> pks;
    private List<ForeignKey> fks;
    private List<String> attributes;
    private List<String> keys;

    public String getName() {
        return name;
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

    public Table(String name, List<String> pks, List<ForeignKey> fks, List<String> attributes) {
        this.name = name;
        this.pks = pks;
        this.fks = fks;
        this.attributes = attributes;
        this.keys = new ArrayList<>();
    }

    public Table(String name, List<String> pks, List<ForeignKey> fks, List<String> attributes, List<String> keys) {
        this.name = name;
        this.pks = pks;
        this.fks = fks;
        this.attributes = attributes;
        this.keys = keys;
    }  
    
}
