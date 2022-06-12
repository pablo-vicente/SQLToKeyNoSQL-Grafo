/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import util.*;
import util.SQL.JoinStatment;
import util.SQL.Table;
import util.joins.HashJoin;
import util.joins.InMemoryJoins;
import util.operations.Operator;

/**
 *
 * @author geomar
 */
public class ExecutionEngine {

    //private BDR bd;
    private final Dictionary dic;

    public ExecutionEngine() {
        dic = loadDictionary();
    }

    private Dictionary loadDictionary() {
        Dictionary d = DictionaryDAO.loadDictionary();
        if (d == null) {
            return new Dictionary();
        }
        //changeCurrentDB(d.getCurrent_db().getName());
        return d;
    }

    private void createDBR(String name) {
        //bd = ;
        dic.getBdrs().add(new BDR(name, new ArrayList<Table>()));
    }

    public void changeCurrentDB(String name) {
        dic.setCurrent_db(name);
        // bd = ;
        if (dic.getCurrent_db() == null) {
            System.out.println("New RDB " + name + " created!");
            this.createDBR(name);
        }
        dic.setCurrent_db(name);
    }

    public boolean createTable(Table t) {
        if (dic.getCurrent_db().getTables() != null) {
            dic.getCurrent_db().getTables().add(t);
            return true;
        }
        return false;
    }

    public boolean insertData(String table, LinkedList<String> columns, ArrayList<String> values) {
        Table tableDb = dic.getCurrent_db().getTable(table);
        if (tableDb == null) {
            System.out.println("Tabela não Existe!");
            return false;
        }
        boolean equal = true;
        String key = "";
        for (String k : tableDb.getPks()) {
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
        long now = new Date().getTime();

        NoSQL targetDb = tableDb.getTargetDB();
        Connector connection = targetDb.getConection();
        connection.put(tableDb, key, columns, values);

        TimeConter.current += (new Date().getTime()) - now;
        tableDb.getKeys().add(key);
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
            Table t = dic.getCurrent_db().getTable(s);
            if (t == null) {
                System.out.println("Tabela: " + s + " não existe!");
                return null;
            }
            tables.add(t);
        }
        for (Table t : tables) {
            for (String k : t.getKeys()) {
                HashMap<String, String> _new = new <String, String>HashMap();
                long now = new Date().getTime();
                aux = t.getTargetDB().getConection().get(1, t.getName(), k);
                TimeConter.current += (new Date().getTime()) - now;
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

    void deleteData(String table, Stack<Object> filters) {
        //ArrayList<Table> tables = new <Table>ArrayList();

        Table t = dic.getCurrent_db().getTable(table);
        System.out.print("Table: " + table + ", r: " + t.getKeys().size());
        //t.setKeys(new ArrayList<String>());
        LinkedList <String> cols = new LinkedList();
        ArrayList <String> tables = new ArrayList();
        tables.add(table);
        cols.add("_key");
        DataSet ds = getDataSetBl(tables, cols, filters);
        for(String [] tuple : ds.getData()){
            t.getTargetDB().getConection().delete(table, tuple[0]);
            t.getKeys().remove(tuple[0]);
        }
        
        System.out.println(" new: " + t.getKeys().size());
    }
    
    void updateData(String table, ArrayList acls, ArrayList avl,Stack<Object> filters) {
        //ArrayList<Table> tables = new <Table>ArrayList();

        Table t = dic.getCurrent_db().getTable(table);
        System.out.print("Table: " + table + ", r: " + t.getKeys().size());
        //t.setKeys(new ArrayList<String>());
        LinkedList <String> cols = t.getAttributes();
        ArrayList <String> tables = new ArrayList();
        tables.add(table);
        cols.add("_key");
        DataSet ds = getDataSetBl(tables, cols, filters);
        for(String [] tuple : ds.getData()){
            ArrayList<String> val = new ArrayList();
            for(int i=0;i<acls.size();i++){
                tuple[cols.indexOf(acls.get(i))] = (String) avl.get(i);
            }
            for (int i = 0; i<cols.size()-1;i++){
                val.add(tuple[i]);
            }
            String k = tuple[cols.size()-1];
            t.getTargetDB().getConection().delete(table, k);
            //tuple[cols.indexOf()]
            cols.remove("_key");
            t.getTargetDB().getConection().put(t, k, cols, val);
        }
        
        System.out.println(" new: " + t.getKeys().size());
    }

    DataSet getDataSet(List<String> tablesN, List<String> cols, Stack<Object> filters) {
        DataSet result = null;
        HashMap<String, String> aux;
        ArrayList<Table> tables = new <Table>ArrayList();

        for (String s : tablesN) {
            Table t = dic.getCurrent_db().getTable(s);
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
            result.setColumns((LinkedList) cols);
            for (String k : t.getKeys()) {
                String[] tuple = new String[cols.size()];
                //HashMap<String, String> _new = new <String, String>HashMap();
                long now = new Date().getTime();
                aux = t.getTargetDB().getConection().get(1, t.getName(), k);
                TimeConter.current += (new Date().getTime()) - now;
                if (applyFilterR((filters != null ? (Stack) filters.clone() : null), aux)) {
                    for (int i = 0; i < cols.size(); i++) {
                        tuple[i] = aux.get(cols.get(i));
                    }
                    result.getData().add(tuple);
                }

            }

        }

        return result; //To change body of generated methods, choose Tools | Templates.
    }

    DataSet getDataSetBl(List<String> tablesN, List<String> cols, Stack<Object> filters) {
        DataSet result = null;
        ArrayList<HashMap<String, String>> tuples;
        HashMap<String, String> aux;
        ArrayList<Table> tables = new <Table>ArrayList();

        for (String s : tablesN) {
            Table t = dic.getCurrent_db().getTable(s);
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
            result.setColumns((LinkedList<String>)cols);
            long now = new Date().getTime();
            result.setData(t.getTargetDB().getConection().getN(1, t.getName(), t.getKeys(),filters, (LinkedList) cols));
            TimeConter.current += (new Date().getTime()) - now;
                
//            for (HashMap<String, String> tpl: tuples) {
//                String[] tuple = new String[cols.size()];
//                //HashMap<String, String> _new = new <String, String>HashMap();
//                aux = tpl;
//               // if (applyFilterR((filters != null ? (Stack) filters.clone() : null), aux)) {
//                    for (int i = 0; i < cols.size(); i++) {
//                        tuple[i] = aux.get(cols.get(i));
//                    }
//                    result.getData().add(tuple);
//               // }
//
//            }

        }

        return result; //To change body of generated methods, choose Tools | Templates.
    }

    
    private boolean applyFilter(List<Object> filters, HashMap tuple) {
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

    private boolean applyFilterR(Stack<Object> filters, HashMap tuple) {
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
            Object val = null;
            Operator op = null;
            op = ((Operator) o);
            val = filters.pop();
            col = ((Column) filters.pop()).getColumnName();
            result = compare((String) tuple.get(col), op, val);
        }
        return result;
    }

    private boolean compare(String v1, Operator operation, Object val) {
        if (v1 == null || val == null) {
            return false;
        }
        return operation.compare(v1, val);
    }

    public BDR getBd() {
        return dic.getCurrent_db();
    }

    public Dictionary getDic() {
        return dic;
    }

    DataSet getDataSet(List<String> tablesN, LinkedList<String> cols, Stack<Object> filters, List<Join> joins) {
        DataSet innerData, result = null;
        HashMap<String, String> aux;
        ArrayList<Table> tables = new <Table>ArrayList();

        for (String s : tablesN) {
            Table t = dic.getCurrent_db().getTable(s);
            if (t == null) {
                System.out.println("Tabela: " + s + " não existe!");
                return null;
            }
            tables.add(t);
        }

        innerData = new DataSet();
        //buscando e manipulando só a inner table (mais da esquerda...)
        Table inner = tables.get(0);
        innerData.setTable_n(inner.getName());
        LinkedList<String> innerCols = new LinkedList();
        for (String col : cols) {
            String[] c = col.split("\\.");
            if (c.length > 0) {
                if (c[0].equals(inner.getName())) {
                    if (c[1].equals("*")) {
                        for (String a : inner.getAttributes()) {
                            innerCols.add(a);
                        }
                    } else {
                        innerCols.add(c[1]);
                    }
                }
            } else {
                System.out.println("Colunas podem ser duplas...");
                return null;
            }
        }
//        for (String k : inner.getKeys()) {
//            String[] tuple = new String[innerCols.size()];
//            //HashMap<String, String> _new = new <String, String>HashMap();
//            long now = new Date().getTime();
//            aux = inner.getTargetDB().getConection().get(1, inner.getName(), k);
//            TimeConter.current += (new Date().getTime()) - now;
//            // if (applyFilterR((filters != null ? (Stack) filters.clone() : null), aux)) {
//            for (int i = 0; i < innerCols.size(); i++) {
//                tuple[i] = aux.get((innerCols.get(i).split("\\.")[1]));
//            }
//            innerData.getData().add(tuple);
//            //}
//
//        }

        if (filters != null)
            innerData.setData(inner.getTargetDB().getConection().getN(0, inner.getName(), inner.getKeys(), (Stack)filters.clone(), innerCols));
        else
            innerData.setData(inner.getTargetDB().getConection().getN(0, inner.getName(), inner.getKeys(), null, innerCols));
        
        innerData.setColumns(innerCols);
        //fim inner
        InMemoryJoins join = new HashJoin();
        
        for (Join j : joins) {
            DataSet outerData = new DataSet();
            Stack<Object> on = null;
            if (j.getOnExpression() != null) {
                Expression e = j.getOnExpression();
                ExpressionDeParser deparser = new JoinStatment();
                StringBuilder b = new StringBuilder();
                deparser.setBuffer(b);
                e.accept(deparser);
                on = ((JoinStatment) deparser).getParsedFilters();
                Table outer = dic.getCurrent_db().getTable(((net.sf.jsqlparser.schema.Table) j.getRightItem()).getName());

                    LinkedList<String> outerCols = new LinkedList();
                for (String col : cols) {
                    String[] c = col.split("\\.");
                    if (c.length > 0) {
                        if (c[0].equals(outer.getName())) {
                            if (c[1].equals("*")) {
                                for (String a : outer.getAttributes()) {
                                    outerCols.add( a);
                                }
                            } else {
                                outerCols.add(c[1]);
                            }
                        }
                    } else {
                        System.out.println("Colunas podem ser duplas...");
                        return null;
                    }
                }
//                for (String k : outer.getKeys()) {
//                    String[] tuple = new String[outerCols.size()];
//                    //HashMap<String, String> _new = new <String, String>HashMap();
//                    long now = new Date().getTime();
//                    aux = outer.getTargetDB().getConection().get(1, outer.getName(), k);
//                    TimeConter.current += (new Date().getTime()) - now;
//                    // if (applyFilterR((filters != null ? (Stack) filters.clone() : null), aux)) {
//                    for (int i = 0; i < outerCols.size(); i++) {
//                        tuple[i] = aux.get((outerCols.get(i).split("\\.")[1]));
//                    }
//                    outerData.getData().add(tuple);
//                    //}
//
//                }

                if (filters != null)
                    outerData.setData(outer.getTargetDB().getConection().getN(0, outer.getName(), outer.getKeys(), (Stack)filters.clone(), outerCols));
                else
                    outerData.setData(outer.getTargetDB().getConection().getN(0, outer.getName(), outer.getKeys(), null, outerCols));
                
                outerData.setTable_n(outer.getName());
                outerData.setColumns(outerCols);
            } else {
                System.out.println("Problemas no JOIN!");
                return null;
            }
            
            result = join.join(innerData, outerData, on);
            innerData = result;
        }
        
        return result;
    }
}
