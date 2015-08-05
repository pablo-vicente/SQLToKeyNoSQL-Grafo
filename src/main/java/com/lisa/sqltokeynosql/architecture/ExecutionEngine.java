/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import util.BDR;
import util.Dicionary;
import util.Table;

/**
 *
 * @author geomar
 */
public class  ExecutionEngine {
    private BDR bd;

    public ExecutionEngine() {
        //bd = new BDR("teste", null)
    }
    
    public void createDBR(String name){
        Dicionary dicionary = new Dicionary();
        bd = new BDR("teste", new ArrayList<Table>());
        dicionary.getBdrs().add(bd);
    }
    
    public boolean createTable(Table t){
        if (bd.getTables() != null){
            bd.getTables().add(t);
            return true;
        }
        return false;
    }
    
    public boolean insertData(String table, ArrayList <String> columns, ArrayList <String> values ){
        Table t = bd.getTable(table);
        if (t == null){
            System.out.println("Tabela não Existe!");
            return false;
        }
        boolean equal = true;
        String key="";    
        for (String k : t.getPks()){
            equal = false;
            for(String aux: columns){    
                if (k.equals(aux)){
                    key+=(key.length()>0?"_":"")+values.get(columns.indexOf(aux));
                    equal = true;
                    break;
                 }
            }
            if (!equal){
                System.out.println("Falta uma pk");
                return false;
            }
        }
        
        t.getTargetDB().getConection().put(table, key, columns, values);
        t.getKeys().add(key);
        return true;
    }
    
    public boolean deleteData(String table, ArrayList <String> columns, ArrayList <String> values ){
        
        return false;
    }
}
