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
    
    public abstract void delete(String table, String key);
    
    public abstract HashMap<String, String> get(int n, String t,String key);
    
    public ArrayList<HashMap<String, String>> getN(int n, String t,ArrayList<String> keys){
        ArrayList<HashMap<String, String>> result = new ArrayList();
        for(String key: keys){
            result.add(get(n, t, key));
        }
        return result;
    }
    
}
