/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import com.lisa.sqltokeynosql.util.Dictionary;
import com.lisa.sqltokeynosql.util.*;
import com.lisa.sqltokeynosql.util.joins.HashJoin;
import com.lisa.sqltokeynosql.util.sql.JoinStatement;
import com.lisa.sqltokeynosql.util.sql.Table;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toList;

/**
 * @author geomar
 */
public class ExecutionEngine {

    private final Dictionary dictionary;
    private static final ExecutorService threadPool = Executors.newCachedThreadPool();

    public ExecutionEngine() {
        dictionary = DictionaryDAO.loadDictionary()
                        .orElseGet(Dictionary::new);
    }

    private void createDBR(final String name) {
        dictionary.getRdbms().add(new BDR(name, new ArrayList<>()));
        DictionaryDAO.storeDictionary(dictionary);
    }

    public void changeCurrentDB(final String name) {
        dictionary.setCurrentDb(name);
        if (dictionary.getCurrentDb() == null) {
            System.out.println("New RDB " + name + " created!");
            this.createDBR(name);
        }
        dictionary.setCurrentDb(name);
        DictionaryDAO.storeDictionary(dictionary);
    }

    public boolean createTable(final Table table) {
        if (dictionary.getCurrentDb().getTables() != null) {
            dictionary.getCurrentDb().getTables().add(table);
            DictionaryDAO.storeDictionary(dictionary);
            return true;
        }
        return false;
    }

    public boolean insertData(final String tableName, final LinkedList<String> columns, final ArrayList<String> values) {
        Optional<Table> optionalTable = dictionary.getCurrentDb().getTable(tableName);
        if (optionalTable.isEmpty()) {
            System.out.println("Tabela não Existe!");
            return false;
        }
        Table table = optionalTable.get();
        String key = "";
        for (String k : table.getPks()) {
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
        table.getTargetDB().getConnection().put(tableName, key, columns, values);
        TimeConter.current = (new Date().getTime()) - now;
        table.getKeys().add(key);
        return true;
    }

    void deleteData(final String table, final Stack<Object> filters) {
        Table t = dictionary.getCurrentDb().getTable(table).get();
        System.out.print("Table: " + table + ", r: " + t.getKeys().size());
        LinkedList<String> cols = new LinkedList<>();
        ArrayList<String> tables = new ArrayList<>();
        tables.add(table);
        cols.add("_key");
        DataSet ds = getDataSetBl(tables, cols, filters);
        for (String[] tuple : ds.getData()) {
            t.getTargetDB().getConnection().delete(table, tuple[0]);
            t.getKeys().remove(tuple[0]);
        }

        System.out.println(" new: " + t.getKeys().size());
    }

    void updateData(final String tableName, final ArrayList acls, final ArrayList avl, final Stack<Object> filters) {
        dictionary
                .getTable(tableName)
                .ifPresent(table -> updateTable(tableName, acls, avl, filters, table));
    }

    private void updateTable(String tableName, ArrayList acls, ArrayList avl, Stack<Object> filters, Table table) {
        System.out.print("Table: " + tableName + ", r: " + table.getKeys().size());
        List<String> cols = table.getAttributes();
        ArrayList<String> tables = new ArrayList<>();
        tables.add(tableName);
        cols.add("_key");
        DataSet ds = getDataSetBl(tables, cols, filters);
        for (String[] tuple : ds.getData()) {
            for (int i = 0; i < acls.size(); i++) {
                tuple[cols.indexOf(acls.get(i))] = (String) avl.get(i);
            }
            ArrayList<String> values = new ArrayList<>(Arrays.asList(tuple).subList(0, cols.size() - 1));
            String k = tuple[cols.size() - 1];
            table.getTargetDB().getConnection().delete(tableName, k);
            cols.remove("_key");
            table.getTargetDB().getConnection().put(tableName, k, (LinkedList<String>) cols, values);
        }

        System.out.println(" new: " + table.getKeys().size());
    }

    DataSet getDataSetBl(final List<String> tableNames, final List<String> cols, final Stack<Object> filters) {
        return tableNames
                .parallelStream()
                .map(dictionary::getTable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(table -> {
                    DataSet dataSet = new DataSet();
                    dataSet.setColumns(getColumnsForResultingDataSet(cols, table));
                    long now = new Date().getTime();
                    dataSet.setData(getNWrapper(table, (LinkedList<String>) getColumnsForResultingDataSet(cols, table), filters, 0));
                    TimeConter.current = (new Date().getTime()) - now;
                    return dataSet;
                }).findAny().orElse(null);
    }

    private List<String> getColumnsForResultingDataSet(final List<String> cols, final Table table) {
        if (cols.get(0).equals("*")) {
            return table.getAttributes();
        }
        return cols;
    }

    private ArrayList getNWrapper(final Table table, final LinkedList columns, final Stack<Object> filters, final int n) {
        return table.getTargetDB().getConnection().getN(n, table.getName(), (ArrayList<String>) table.getKeys(), filters, columns);
    }

    DataSet getDataSet(final List<String> tableNames, final LinkedList<String> columns, final Stack<Object> filters, final List<Join> joins) {

        DataSet innerData = getInnerDataSet(tableNames, columns, filters);
        if (innerData == null) return null;


        List<Callable<Result>> jobs = new ArrayList<>();
        for (Join j : joins) {
            jobs.add(() -> getOuterData(columns, filters, j));
        }

        List<Result> data = getFutures(jobs).stream().map(this::getCompleted).collect(toList());

        DataSet result = new DataSet();

        for (Result res : data) {
            HashJoin join = new HashJoin();
            result = join.join(innerData, res.outerData, res.on);
            innerData = result; //mutability
        }

        return result;
    }

    private List<Future<Result>> getFutures(List<Callable<Result>> jobs) {
        try {
            return threadPool.invokeAll(jobs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Result getCompleted(Future<Result> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Result getOuterData(LinkedList<String> columns, Stack<Object> filters, Join j) {
        DataSet outerData = new DataSet();
        Stack<Object> on;
        if (j.getOnExpression() != null) {
            Expression e = j.getOnExpression();
            ExpressionDeParser deparser = new JoinStatement();
            StringBuilder b = new StringBuilder();
            deparser.setBuffer(b);
            e.accept(deparser);
            on = ((JoinStatement) deparser).getParsedFilters();
            Table outer = dictionary.getCurrentDb().getTable(((net.sf.jsqlparser.schema.Table) j.getRightItem()).getName()).get();

            LinkedList<String> outerCols = new LinkedList();
            for (String col : columns) {
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
                    System.out.println("Colunas nao podem ser duplas...");
                    return null;
                }
            }
            if (filters != null)
                outerData.setData(getNWrapper(outer, outerCols, (Stack) filters.clone(), 0));
            else
                outerData.setData(getNWrapper(outer, outerCols, null, 0));


            outerData.setTableName(outer.getName());
            outerData.setColumns(outerCols);
        } else {
            System.out.println("JOIN without ON clause!!");
            return null;
        }

        return new Result(on, outerData);
    }

    public NoSQL getTarget(String schemaName) {
        return dictionary.getTarget(schemaName);
    }

    public void addTarget(NoSQL noSQL) {
        dictionary.getTargets().add(noSQL);
        dictionary.getRdbms().add(new BDR(noSQL.getAlias(), new ArrayList<>()));
        DictionaryDAO.storeDictionary(dictionary);
    }

    public List<NoSQL> getTargets() {
        return dictionary.getTargets();
    }

    public BDR getCurrentDb() {
        return dictionary.getCurrentDb();
    }

    public List<BDR> getRdbms() {
        return dictionary.getRdbms();
    }

    public void setCurrentDb(String currentDataBase) {
        dictionary.setCurrentDb(currentDataBase);
        DictionaryDAO.storeDictionary(dictionary);
    }

    static class Result {
        public Stack<Object> on;
        public DataSet outerData;

        public Result(Stack<Object> on, DataSet data) {
            this.on = on;
            this.outerData = data;
        }
    }

    private DataSet getInnerDataSet(List<String> tableNames, LinkedList<String> columns, Stack<Object> filters) {
        List<Table> tables = tableNames
                .stream()
                .map(dictionary::getTable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        DataSet innerData = new DataSet();
        Table inner = tables.get(0);
        innerData.setTableName(inner.getName());
        LinkedList<String> innerCols = new LinkedList<>();
        for (String column : columns) {
            String[] c = column.split("\\.");
            if (c.length > 0) {
                if (c[0].equals(inner.getName())) {
                    if (c[1].equals("*")) {
                        innerCols.addAll(inner.getAttributes());
                    } else {
                        innerCols.add(c[1]);
                    }
                }
            } else {
                System.out.println("Colunas podem ser duplas...");
                return null;
            }
        }

        innerData.setColumns(innerCols);

        if (filters != null) {
            innerData.setData(getNWrapper(inner, innerCols, (Stack<Object>) filters.clone(), 0));
        } else {
            innerData.setData(getNWrapper(inner, innerCols, null, 0));
        }
        return innerData;
    }
}
