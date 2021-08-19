/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import util.Dictionary;
import util.*;
import util.SQL.JoinStatement;
import util.SQL.Table;
import util.joins.HashJoin;
import util.joins.InMemoryJoins;
import util.operations.Operator;

import java.util.*;

/**
 * @author geomar
 */
public class ExecutionEngine {

    private final Dictionary dictionary;

    public ExecutionEngine() {
        dictionary = loadDictionary();
    }

    private Dictionary loadDictionary() {
        Dictionary d = DictionaryDAO.loadDictionary();
        if (d == null) {
            return new Dictionary();
        }
        return d;
    }

    private void createDBR(String name) {
        dictionary.getBdrs().add(new BDR(name, new ArrayList<>()));
    }

    public void changeCurrentDB(String name) {
        dictionary.setCurrent_db(name);
        if (dictionary.getCurrent_db() == null) {
            System.out.println("New RDB " + name + " created!");
            this.createDBR(name);
        }
        dictionary.setCurrent_db(name);
    }

    public boolean createTable(Table t) {
        if (dictionary.getCurrent_db().getTables() != null) {
            dictionary.getCurrent_db().getTables().add(t);
            return true;
        }
        return false;
    }

    public boolean insertData(String table, LinkedList<String> columns, ArrayList<String> values) {
        Table t = dictionary.getCurrent_db().getTable(table);
        if (t == null) {
            System.out.println("Tabela não Existe!");
            return false;
        }
        String key = "";
        for (String k : t.getPks()) {
            boolean equal = false;
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
        t.getTargetDB().getConection().put(table, key, columns, values);
        TimeConter.current += (new Date().getTime()) - now;
        t.getKeys().add(key);
        return true;
    }

    void deleteData(String table, Stack<Object> filters) {
        Table t = dictionary.getCurrent_db().getTable(table);
        System.out.print("Table: " + table + ", r: " + t.getKeys().size());
        LinkedList<String> cols = new LinkedList();
        ArrayList<String> tables = new ArrayList();
        tables.add(table);
        cols.add("_key");
        DataSet ds = getDataSetBl(tables, cols, filters);
        for (String[] tuple : ds.getData()) {
            t.getTargetDB().getConection().delete(table, tuple[0]);
            t.getKeys().remove(tuple[0]);
        }

        System.out.println(" new: " + t.getKeys().size());
    }

    void updateData(String table, ArrayList acls, ArrayList avl, Stack<Object> filters) {
        Table t = dictionary.getCurrent_db().getTable(table);
        System.out.print("Table: " + table + ", r: " + t.getKeys().size());
        LinkedList<String> cols = t.getAttributes();
        ArrayList<String> tables = new ArrayList();
        tables.add(table);
        cols.add("_key");
        DataSet ds = getDataSetBl(tables, cols, filters);
        for (String[] tuple : ds.getData()) {
            ArrayList<String> val = new ArrayList();
            for (int i = 0; i < acls.size(); i++) {
                tuple[cols.indexOf(acls.get(i))] = (String) avl.get(i);
            }
            for (int i = 0; i < cols.size() - 1; i++) {
                val.add(tuple[i]);
            }
            String k = tuple[cols.size() - 1];
            t.getTargetDB().getConection().delete(table, k);
            cols.remove("_key");
            t.getTargetDB().getConection().put(table, k, cols, val);
        }

        System.out.println(" new: " + t.getKeys().size());
    }

    DataSet getDataSetBl(List<String> tablesN, List<String> cols, Stack<Object> filters) {
        DataSet result;
        ArrayList<Table> tables = new ArrayList<>();

        for (String s : tablesN) {
            Table t = dictionary.getCurrent_db().getTable(s);
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
            result.setColumns((LinkedList<String>) cols);
            long now = new Date().getTime();
            result.setData(t.getTargetDB().getConection().getN(1, t.getName(), t.getKeys(), filters, (LinkedList) cols));
            TimeConter.current += (new Date().getTime()) - now;
        }

        return result;
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    DataSet getDataSet(List<String> tableNames, LinkedList<String> cols, Stack<Object> filters, List<Join> joins) {
        DataSet innerData, result = null;
        ArrayList<Table> tables = new ArrayList<>();

        for (String s : tableNames) {
            Table t = dictionary.getCurrent_db().getTable(s);
            if (t == null) {
                System.out.println("Tabela: " + s + " não existe!");
                return null;
            }
            tables.add(t);
        }

        innerData = new DataSet();
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

        if (filters != null)
            innerData.setData(inner.getTargetDB().getConection().getN(0, inner.getName(), inner.getKeys(), (Stack) filters.clone(), innerCols));
        else
            innerData.setData(inner.getTargetDB().getConection().getN(0, inner.getName(), inner.getKeys(), null, innerCols));

        innerData.setColumns(innerCols);
        InMemoryJoins join = new HashJoin();

        for (Join j : joins) {
            DataSet outerData = new DataSet();
            Stack<Object> on = null;
            if (j.getOnExpression() != null) {
                Expression e = j.getOnExpression();
                ExpressionDeParser deparser = new JoinStatement();
                StringBuilder b = new StringBuilder();
                deparser.setBuffer(b);
                e.accept(deparser);
                on = ((JoinStatement) deparser).getParsedFilters();
                Table outer = dictionary.getCurrent_db().getTable(((net.sf.jsqlparser.schema.Table) j.getRightItem()).getName());

                LinkedList<String> outerCols = new LinkedList();
                for (String col : cols) {
                    String[] c = col.split("\\.");
                    if (c.length > 0) {
                        if (c[0].equals(outer.getName())) {
                            if (c[1].equals("*")) {
                                for (String a : outer.getAttributes()) {
                                    outerCols.add(a);
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
                if (filters != null)
                    outerData.setData(outer.getTargetDB().getConection().getN(0, outer.getName(), outer.getKeys(), (Stack) filters.clone(), outerCols));
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
