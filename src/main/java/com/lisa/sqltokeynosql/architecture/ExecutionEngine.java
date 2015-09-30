/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import util.BDR;
import util.DataSet;
import util.Dictionary;
import util.DictionaryDAO;
import util.JoinStatment;
import util.Table;
import util.TimeConter;
import util.WhereStatment;
import util.joins.InMemoryJoins;
import util.joins.NestedLoop;
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

    public boolean insertData(String table, ArrayList<String> columns, ArrayList<String> values) {
        Table t = dic.getCurrent_db().getTable(table);
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
        long now = new Date().getTime();
        t.getTargetDB().getConection().put(table, key, columns, values);
        TimeConter.current += (new Date().getTime()) - now;
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

    void deleteData(String table) {
        //ArrayList<Table> tables = new <Table>ArrayList();

        Table t = dic.getCurrent_db().getTable(table);
        System.out.print("Table: " + table + ", r: " + t.getKeys().size());
        t.setKeys(new ArrayList<String>());
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
        /*Stack f  =  (Stack) filters.clone();
         System.out.println("FIltros -- "+f.size());
         Object o = null;
         for (;!f.empty(); ) {
         o = f.pop();
         System.out.print("-- ");
         if (o instanceof AndExpression) {
         System.out.println("AND");
         } else if (o instanceof OrExpression) {
         System.out.println("OR");
         } else {
         System.out.println(o.toString());
         }

         }
         System.out.println("------");
         */

        result = new DataSet();
        for (Table t : tables) {
            if (cols.get(0).equals("*")) {
                cols = t.getAttributes();
            }
            result.setColumns((ArrayList) cols);
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

    DataSet getDataSet(List<String> tablesN, ArrayList<String> cols, Stack<Object> filters, List<Join> joins) {
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
        ArrayList<String> innerCols = new ArrayList();
        for (String col : cols) {
            String[] c = col.split("\\.");
            if (c.length > 0) {
                if (c[0].equals(inner.getName())) {
                    if (c[1].equals("*")) {
                        for (String a : inner.getAttributes()) {
                            innerCols.add(c[0] + "." + a);
                        }
                    } else {
                        innerCols.add(col);
                    }
                }
            } else {
                System.out.println("Colunas podem ser duplas...");
                return null;
            }
        }
        for (String k : inner.getKeys()) {
            String[] tuple = new String[innerCols.size()];
            //HashMap<String, String> _new = new <String, String>HashMap();
            long now = new Date().getTime();
            aux = inner.getTargetDB().getConection().get(1, inner.getName(), k);
            TimeConter.current += (new Date().getTime()) - now;
            // if (applyFilterR((filters != null ? (Stack) filters.clone() : null), aux)) {
            for (int i = 0; i < innerCols.size(); i++) {
                tuple[i] = aux.get((innerCols.get(i).split("\\.")[1]));
            }
            innerData.getData().add(tuple);
            //}

        }
        innerData.setColumns(innerCols);
        //fim inner
        InMemoryJoins join = new NestedLoop();
        
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

                    ArrayList<String> outerCols = new ArrayList();
                for (String col : cols) {
                    String[] c = col.split("\\.");
                    if (c.length > 0) {
                        if (c[0].equals(outer.getName())) {
                            if (c[1].equals("*")) {
                                for (String a : outer.getAttributes()) {
                                    outerCols.add(c[0] + "." + a);
                                }
                            } else {
                                outerCols.add(col);
                            }
                        }
                    } else {
                        System.out.println("Colunas podem ser duplas...");
                        return null;
                    }
                }
                for (String k : outer.getKeys()) {
                    String[] tuple = new String[outerCols.size()];
                    //HashMap<String, String> _new = new <String, String>HashMap();
                    long now = new Date().getTime();
                    aux = outer.getTargetDB().getConection().get(1, outer.getName(), k);
                    TimeConter.current += (new Date().getTime()) - now;
                    // if (applyFilterR((filters != null ? (Stack) filters.clone() : null), aux)) {
                    for (int i = 0; i < outerCols.size(); i++) {
                        tuple[i] = aux.get((outerCols.get(i).split("\\.")[1]));
                    }
                    outerData.getData().add(tuple);
                    //}

                }
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
