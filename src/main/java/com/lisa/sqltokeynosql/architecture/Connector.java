/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import util.SQL.Table;
import util.operations.Operator;

/**
 *
 * @author geomar
 */
public abstract class Connector {
    
    public abstract void connect(String nbd);
    
    public abstract  void put (Table table, String key, LinkedList<String>cols, ArrayList<String>values);
    
    public abstract void delete(String table, String key);
    
    public abstract HashMap<String, String> get(int n, String t,String key);
    
    public ArrayList<HashMap<String, String>> getN(int n, String t,ArrayList<String> keys){
        ArrayList<HashMap<String, String>> result = new ArrayList();
        for(String key: keys){
            result.add(get(n, t, key));
        }
        return result;
    }
    
    public ArrayList<HashMap<String, String>> getN(int n, String t,ArrayList<String> keys, Stack<Object> filters){
        ArrayList<HashMap<String, String>> result = new ArrayList();
        for(String key: keys){
            HashMap<String, String> tuple = get(n, t, key);
            if (applyFilterR(filters, tuple))
              result.add(tuple);
        }
        return result;
    }
    
    public ArrayList getN(int n, String t,ArrayList<String> keys, Stack<Object> filters, LinkedList<String> cols){
        ArrayList<String[]> result = new ArrayList();
        for(String key: keys){
            HashMap<String, String> tuple = get(n, t, key);
            if (applyFilterR(filters, tuple)){
                String[] tupleR = new String[cols.size()];
                for(int i = 0; i < cols.size();i++){
                    tupleR[i] = (String)tuple.get(cols.get(i));
                }
                result.add(tupleR);
            }
        }
        return result;
    }
    
    protected boolean applyFilter(List<Object> filters, HashMap tuple) {
        if (filters == null) {
            return true;
        }
        Boolean result = false;
        String col = null;
        Object val = null;
        Operator op = null;
        for (Object o : filters) {
            if (o instanceof Column) {
                if (col == null) {
                    col = ((Column) o).getColumnName();
                } else {
                    System.out.println("Comparação de colunas;");
                }
            } else if (o instanceof Operator) {
                if (op == null) {
                    op = ((Operator) o);
                } else {
                    System.out.println("Comparação ja setada;");
                }
            } else {
                if (val == null) {
                    val = (o);
                } else {
                    System.out.println("Comparação de valores;");
                }
            }

            if (col != null && op != null && val != null) {
                result = compare((String) tuple.get(col), op, val);
                col = null;
                op = null;
                val = null;
            }
        }

        return result;
    }

    protected boolean applyFilterR(Stack<Object> filters, HashMap tuple) {
        if (filters == null) {
            return true;
        }
        Boolean result = false;
        Object o = filters.pop();
        if (o instanceof AndExpression) {
            result = applyFilterR(filters, tuple);
            if (!result) {
                return false;
            }
            result = (result && applyFilterR(filters, tuple));
        } else if (o instanceof OrExpression) {
            result = applyFilterR(filters, tuple);
            result = (result || applyFilterR(filters, tuple));
        } else {
            String col = null;
            Object val = Parser.removeInvalidCaracteres(filters.pop().toString());
            Operator op = null;
            op = ((Operator) o);
            col = ((Column) filters.pop()).getColumnName();
            result = compare((String) tuple.get(col), op, val);
        }
        return result;
    }
    
        protected boolean compare(String v1, Operator operation, Object val) {
        if (v1 == null || val == null) {
            return false;
        }
        return operation.compare(v1, val);
    }

}
