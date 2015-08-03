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
}
