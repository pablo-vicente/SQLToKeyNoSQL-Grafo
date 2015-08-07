/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import java.util.HashMap;
import org.bson.Document;

/**
 *
 * @author geomar
 */
public abstract class Connector {
    
    public abstract void connect(String nbd);
    
    public abstract  void put (String table, String key, ArrayList<String>cols, ArrayList<String>values);
    
    public abstract void delete();
    
    public abstract HashMap<String, String> get(int n, String t,String key);
    
}
