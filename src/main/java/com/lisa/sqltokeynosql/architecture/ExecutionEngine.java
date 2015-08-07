/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import util.BDR;
import util.DataSet;
import util.Dicionary;
import util.Table;
import util.operations.Operator;

/**
 *
 * @author geomar
 */
public class ExecutionEngine {

    private BDR bd;

    public ExecutionEngine() {
        //bd = new BDR("teste", null)
    }

    public void createDBR(String name) {
        Dicionary dicionary = new Dicionary();
        bd = new BDR("teste", new ArrayList<Table>());
        dicionary.getBdrs().add(bd);
    }

    public boolean createTable(Table t) {
        if (bd.getTables() != null) {
            bd.getTables().add(t);
            return true;
        }
        return false;
    }

    public boolean insertData(String table, ArrayList<String> columns, ArrayList<String> values) {
        Table t = bd.getTable(table);
        if (t == null) {
            System.out.println("Tabela não Existe!");
            return false;
        }
        boolean equal = true;
        String key = "";
        for (String k : t.getPks()) {
            equal = false;
            for (String aux : columns) {
                if (k.equals(aux)) {
                    key += (key.length() > 0 ? "_" : "") + values.get(columns.indexOf(aux));
                    equal = true;
                    break;
                }
            }
            if (!equal) {
                System.out.println("Falta uma pk");
                return false;
            }
        }

        t.getTargetDB().getConection().put(table, key, columns, values);
        t.getKeys().add(key);
        return true;
    }

    public boolean deleteData(String table, ArrayList<String> columns, ArrayList<String> values) {

        return false;
    }

    ArrayList<HashMap<String, String>> getData(List<String> tablesN, List<String> cols, List<String> filters) {
        ArrayList<HashMap<String, String>> result = new <HashMap<String, String>>ArrayList();
        HashMap<String, String> aux;
        ArrayList<Table> tables = new <Table>ArrayList();
        for (String s : tablesN) {
            Table t = bd.getTable(s);
            if (t == null) {
                System.out.println("Tabela: " + s + " não existe!");
                return null;
            }
            tables.add(t);
        }
        for (Table t : tables) {
            for (String k : t.getKeys()) {
                HashMap<String, String> _new = new <String, String>HashMap();
                aux = t.getTargetDB().getConection().get(1, t.getName(), k);
                for (String c : cols) {
                    if (!c.equals("*")) {
                        _new.put(c, aux.get(c));
                    } else {
                        //cols = t.getAttributes();
                        _new = (HashMap) aux.clone();
                        break;
                    }
                }
                result.add(_new);
            }
        }
        return result; //To change body of generated methods, choose Tools | Templates.
    }

    DataSet getDataSet(List<String> tablesN, List<String> cols, List<Object> filters) {
        DataSet result = null;
        HashMap<String, String> aux;
        ArrayList<Table> tables = new <Table>ArrayList();

        for (String s : tablesN) {
            Table t = bd.getTable(s);
            if (t == null) {
                System.out.println("Tabela: " + s + " não existe!");
                return null;
            }
            tables.add(t);
        }
        result = new DataSet();
        for (Table t : tables) {
            if (cols.get(0).equals("*")) {
                cols = t.getAttributes();
            }
            result.setColumns((ArrayList) cols);
            for (String k : t.getKeys()) {
                String[] tuple = new String[cols.size()];
                //HashMap<String, String> _new = new <String, String>HashMap();
                aux = t.getTargetDB().getConection().get(1, t.getName(), k);
                if (applyFilter(filters, aux)) {
                    for (int i = 0; i < cols.size(); i++) {
                        tuple[i] = aux.get(cols.get(i));
                    }
                    result.getData().add(tuple);
                }
            }

        }
        return result; //To change body of generated methods, choose Tools | Templates.
    }

    private boolean applyFilter(List<Object> filters, HashMap tuple) {
        if (filters == null) return true;
        Boolean result = false;
        String col = null;
        Object val = null;
        Operator op = null;
       // System.out.print("FIltro: "+tuple.get(((Column)filters.get(0)).getColumnName())+" "+ (Operator) filters.get(1)+" "+ filters.get(2));
        for (Object o: filters){
            if (o instanceof Column){
                if (col == null)
                    col = ((Column) o).getColumnName();
                else
                    System.out.println("Comparação de colunas;");
            }else if (o instanceof Operator){
                if (op == null)
                    op = ((Operator) o);
                else
                    System.out.println("Comparação ja setada;");
            }else{
                if (val == null)
                    val = (o);
                else
                    System.out.println("Comparação de valores;");
            }
            
            if (col != null && op != null && val != null){
                result = compare((String) tuple.get(col), op, val);
                col = null;
                op = null;
                val = null;
            }
        }
        
        return result;
    }

    private boolean compare(String v1, Operator operation, Object val) {
        if (v1 == null || val == null)
            return false;
        return operation.compare(v1, val);
    }
}
