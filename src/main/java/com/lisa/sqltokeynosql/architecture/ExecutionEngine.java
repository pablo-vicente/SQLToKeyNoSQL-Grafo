/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import com.lisa.sqltokeynosql.util.sql.ForeignKey;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.alter.AlterExpression;
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

    private void createDBR(final String name)
    {
        dictionary.getRdbms().add(new BDR(name, new ArrayList<>()));
    }

    public void changeCurrentDB(final String name1)
    {
        var name = name1.replace(".", "_");
        var dabaseExists = dictionary
                .getRdbms()
                .stream()
                .anyMatch(br -> br.getName().equalsIgnoreCase(name));
        if (!dabaseExists) {
            System.out.println("New RDB " + name + " created!");
            this.createDBR(name);
        }
        dictionary.setCurrentDb(name);
        SaveDicitionary();
    }

    public void createTable(final Table table)
    {
        var currenteDb = dictionary.getCurrentDb();
        if(currenteDb == null)
            throw new UnsupportedOperationException("Banco de dados não definido!");

        var tables = currenteDb.getTables();

        var tableExist = tables
                .stream()
                .anyMatch(x -> x.getName().equalsIgnoreCase(table.getName()));

        if (tableExist)
            throw new UnsupportedOperationException("A Tabela " + table.getName() + " já foi criada");

        tables.add(table);
        var target = table.getTargetDB();
        var connection = target.getConnection();
        connection.create(table);
    }

    private String getKey(Table tableDb, List<String> columns, List<String> values)
    {
        boolean equal;
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
            if (!equal)
                throw new UnsupportedOperationException("Falta uma pk");
        }

        return key;
    }

    public void insertData(final String tableName, final List<String> columns, final List<List<String>> values) {
        Optional<Table> optionalTable = dictionary.getCurrentDb().getTable(tableName);
        if (optionalTable.isEmpty())
            throw new UnsupportedOperationException("Tabela " +  tableName + " não Existe!");

        Table table = optionalTable.get();

        var dados = new HashMap<String, List<String>>();
        for (List<String> value : values)
        {
            String key = getKey(table, columns, value);
            dados.put(key, value);
        }

        NoSQL targetDb = table.getTargetDB();
        Connector connection = targetDb.getConnection();
        connection.put(table, columns, dados);

        for (var stringListEntry : dados.entrySet())
            table.getKeys().add(stringListEntry.getKey());

    }

    void deleteData(final String table, final Stack<Object> filters) {
        Table table1 = dictionary.getCurrentDb().getTable(table).get();
        ArrayList<String> tables = new ArrayList<>();
        tables.add(table);
        DataSet ds = getDataSetBl(tables, table1.getAttributes(), filters);
        var tuples = ds.getData();

        if(tuples.size() == 0)
            return;

        var keys = new ArrayList<String>();
        for (String[] tuple : tuples)
        {
            ArrayList<String> val = new ArrayList();
            for (int i = 0; i < table1.getAttributes().size();i++)
                val.add(tuple[i]);
            String key = getKey(table1, table1.getAttributes(), val);

            keys.add(key);
        }
        table1
                .getTargetDB()
                .getConnection()
                .delete(table, String.valueOf(keys));

        for (var key : keys)
            table1.getKeys().remove(key);

    }

    void updateData(final String tableName, final ArrayList acls, final ArrayList avl, final Stack<Object> filters) {
        dictionary
                .getTable(tableName)
                .ifPresent(table -> updateTable(tableName, acls, avl, filters, table));
    }

    void dropTable(final String tableName)
    {
        var table = dictionary.getTable(tableName);
        if(table.isEmpty())
            throw new UnsupportedOperationException("Table " + tableName + " Not exists!!!");

        var tableDb = table.get();
        var connector = tableDb
            .getTargetDB()
            .getConnection();

        dictionary
                .getCurrentDb()
                .getTables()
                .remove(table.get());
        connector.drop(tableDb);
    }

    private void updateTable(String tableName, ArrayList acls, ArrayList avl, Stack<Object> filters, Table table)
    {
        for (Object acl : acls)
        {
            var pks = table
                    .getPks()
                    .stream()
                    .anyMatch(x -> x.equalsIgnoreCase((String) acl));

            if(pks)
                throw new UnsupportedOperationException("Não é suportado a mudança de Chave Primaria");
        }

        List<String> cols = table.getAttributes();
        ArrayList<String> tables = new ArrayList<>();
        tables.add(tableName);
        DataSet ds = getDataSetBl(tables, cols, filters);

        var dados = new HashMap<String, ArrayList<String>>();

        for (String[] tuple : ds.getData())
        {
            ArrayList<String> values = new ArrayList<>(Arrays.asList(tuple));
            String key = getKey(table,cols, values);
            for (int i = 0; i < acls.size(); i++)
            {
                var coluna = acls.get(i);
                int indexColuna = cols.indexOf(coluna);
                values.set(indexColuna, (String) avl.get(i));
            }

            dados.put(key, values);
        }

        table
                .getTargetDB()
                .getConnection()
                .update(table, dados);
    }

    DataSet getDataSetBl(final List<String> tableNames, final List<String> cols, final Stack<Object> filters) {
        return tableNames
                .stream()
                .map(dictionary::getTable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(table -> {
                    DataSet dataSet = new DataSet();
                    dataSet.setColumns(getColumnsForResultingDataSet(cols, table));
                    long now = new Date().getTime();

                    var columnsForResultingDataSet = (LinkedList<String>) getColumnsForResultingDataSet(cols, table);
                    var wrapper = getNWrapper(table, columnsForResultingDataSet, filters, 0);
                    dataSet.setData(wrapper);
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
        ArrayList result = table.getTargetDB().getConnection().getN(n, table.getName(), (ArrayList<String>) table.getKeys(), filters, columns);
        return result;
    }

    DataSet getDataSet(final List<String> tableNames, final LinkedList<String> columns, final Stack<Object> filters, final List<Join> joins) {

        List<Table> tables = tableNames
                .stream()
                .map(dictionary::getTable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        Future<DataSet> innerDataJob = threadPool
            .submit(() -> getInnerDataSet(tables.get(0), columns, filters));
        
        List<Callable<Result>> jobs = new ArrayList<>();
        for (Join join : joins) {
            jobs.add(() -> getOuterData(columns, filters, join));
        }
        List<Future<Result>> futures = getFutures(jobs);

        DataSet innerData = completeInnerDataJob(innerDataJob);
        if (innerData == null) return null;
        
        List<Result> data = new ArrayList<>();
        for (Future<Result> job: futures) {
            data.add(getCompleted(job));
        }
        
        DataSet result = new DataSet();

        for (Result res : data) {
            HashJoin join = new HashJoin();
            result = join.join(innerData, res.outerData, res.on);
            innerData = result; //mutability
        }

        return result;
    }

    private DataSet completeInnerDataJob(Future<DataSet> innerDataJob) {
        try {
            return innerDataJob.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            Table outer = dictionary.getTable(getTableName(j)).get();

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

    private String getTableName(Join j) {
        net.sf.jsqlparser.schema.Table table = (net.sf.jsqlparser.schema.Table)j.getRightItem();
        return table.getSchemaName() + "." + table.getName();
    }

    public NoSQL getTarget(String schemaName) {
        return dictionary.getTarget(schemaName);
    }

    public void addTarget(NoSQL noSQL) {
        dictionary.addTarget(noSQL);
        SaveDicitionary();
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

    public void SaveDicitionary()
    {
        DictionaryDAO.storeDictionary(dictionary);
    }

    public void AlterTable(String tablename, List<AlterExpression> alterExpressions)
    {
        var tableOptional = dictionary.getTable(tablename);

        if(tableOptional.isEmpty())
            throw new UnsupportedOperationException("Tabela" + tablename + "inexistente!!!");

        var table = tableOptional.get();
        var cols = table.getAttributes();
        var pks = table.getPks();

        var novasFks = table
                .getFks()
                .stream()
                .map(x -> new ForeignKey(x.getAtt(), x.getrAtt(), x.getrTable()))
                .collect(toList());

        var novosAtributos = new ArrayList<>(table.getAttributes());

        var dados = new ArrayList<AlterDto>();
        for (AlterExpression alterExpression : alterExpressions)
        {
            var alter = alterExpression.getOperation();

            String colunaExistente = "";
            String colunaNova = "";
            boolean chaveEstrangeira = false;
            switch (alter)
            {
                case ADD:
                    colunaNova = alterExpression
                            .getColDataTypeList()
                            .get(0)
                            .getColumnName();

                    validarColunaDuplicada(cols, tablename, colunaNova);
                    novosAtributos.add(colunaNova);
                    break;

                case DROP:
                    colunaExistente = alterExpression.getColumnName();
                    validarColunaInexistente(cols, tablename, colunaExistente);

                    novosAtributos.remove(colunaExistente);
                    String finalColunaExistente = colunaExistente;

                    var novasFksDrop = new ArrayList<>(novasFks);
                    for (ForeignKey foreignKey : novasFksDrop)
                    {
                        if(foreignKey.getAtt().equalsIgnoreCase(finalColunaExistente))
                        {
                            novasFks.remove(foreignKey);
                            chaveEstrangeira = true;
                        }
                    }


                    break;

                case RENAME:

                    colunaExistente = alterExpression.getColumnOldName();
                    colunaNova = alterExpression.getColumnName();

                    validarColunaInexistente(cols, tablename, colunaExistente);
                    validarColunaDuplicada(cols, tablename, colunaNova);

                    var indexAtual = novosAtributos.indexOf(colunaExistente);
                    novosAtributos.set(indexAtual, colunaNova);

                    for (ForeignKey novaFk : novasFks)
                    {
                        var atributeFk = novaFk.getAtt();
                        if(!atributeFk.equalsIgnoreCase(colunaExistente))
                            continue;
                        novaFk.setAtt(colunaNova);
                        chaveEstrangeira = true;
                    }

                    break;

                default:
                    throw new UnsupportedOperationException("Operação Não Suportada!!");
            }

            if(pks.contains(colunaExistente))
                throw new UnsupportedOperationException("Não é permitido alterar chaves primarias!!");

            var dado = new AlterDto(alter, colunaExistente, colunaNova, chaveEstrangeira);
            dados.add(dado);
        }

        var target = table.getTargetDB();
        var connection = target.getConnection();
        connection.alter(table, dados);

        table.setAttributes(novosAtributos);
        table.setFks(novasFks);

    }

    public void validarColunaDuplicada(List<String> cols, String tablename, String colunaNova)
    {
        if(cols.contains(colunaNova))
            throw new UnsupportedOperationException("Coluna "+ colunaNova + " Tabela " + tablename + " Duplicada!!!");

    }

    public void validarColunaInexistente(List<String> cols, String tablename, String colunaExistente)
    {
        if(!cols.contains(colunaExistente))
            throw new UnsupportedOperationException("Coluna "+ colunaExistente + " Tabela " + tablename + " inexistente!!!");
    }

    static class Result {
        public Stack<Object> on;
        public DataSet outerData;

        public Result(Stack<Object> on, DataSet data) {
            this.on = on;
            this.outerData = data;
        }
    }

    private DataSet getInnerDataSet(Table table, LinkedList<String> columns, Stack<Object> filters) {

        DataSet innerData = new DataSet();
        Table innerTable = table;
        innerData.setTableName(innerTable.getName());
        LinkedList<String> innerCols = new LinkedList<>();
        for (String column : columns) {
            String[] c = column.split("\\.");
            if (c.length > 0) {
                if (c[0].equals(innerTable.getName())) {
                    if (c[1].equals("*")) {
                        innerCols.addAll(innerTable.getAttributes());
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
            innerData.setData(getNWrapper(innerTable, innerCols, (Stack<Object>) filters.clone(), 0));
        } else {
            innerData.setData(getNWrapper(innerTable, innerCols, null, 0));
        }
        return innerData;
    }
}
