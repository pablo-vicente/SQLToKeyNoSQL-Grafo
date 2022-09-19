/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import com.lisa.sqltokeynosql.util.TimeReport;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ForeignKeyIndex;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.springframework.stereotype.Service;
import com.lisa.sqltokeynosql.util.BDR;
import com.lisa.sqltokeynosql.util.DataSet;
import com.lisa.sqltokeynosql.util.NoSQL;
import com.lisa.sqltokeynosql.util.sql.ForeignKey;
import com.lisa.sqltokeynosql.util.sql.Table;
import com.lisa.sqltokeynosql.util.sql.WhereStatement;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static java.util.stream.Collectors.toCollection;

/**
 *
 * @author geomar
 */
@Service
public class Parser {

    private final ExecutionEngine executionEngine;
    public final Stack<Integer> stack;

    public Parser() {
        executionEngine = new ExecutionEngine();
        stack = new Stack<>();
        stack.push(1);
    }

    public ArrayList<DataSet> run(final InputStream is) throws JSQLParserException, IOException
    {

        var stopwatch = new org.springframework.util.StopWatch();
        stopwatch.start();
        var dataSets = new ArrayList<DataSet>();
        var br = new BufferedReader(new InputStreamReader(is));
        String line;

        StringBuilder query = new StringBuilder();
        while ((line = br.readLine()) != null)
        {
            var lineClear = line
                    .trim()
                    .toLowerCase();

            if(lineClear.startsWith("--") || lineClear.startsWith("#"))
                continue;

            if(!lineClear.contains(";"))
            {
                query
                        .append(line)
                        .append("\n");
                continue;
            }

            var parte1 = "";
            var parte2 = "";

            if(lineClear.endsWith(";"))
                parte1 = lineClear;
            else
            {
                var partes = lineClear.split(";");
                parte1 = partes[0];
                parte2 = partes[1];
            }

            query
                    .append(parte1)
                    .append("\n");
            var dataSet = run(query.toString());
            if(dataSet != null)
                dataSets.add(dataSet);

            query.setLength(0);
            query.append(parte2);
        }

        if(query.toString().trim() != "")
        {
            var dataSet = run(query.toString().trim());
            query.setLength(0);
            if(dataSet != null)
                dataSets.add(dataSet);
        }

        executionEngine.SaveDicitionary();
        stopwatch.stop();
        TimeReport.TotalSegundos = stopwatch.getTotalTimeSeconds();
        TimeReport.GeneratCsvRepost();
        return dataSets;
    }

    private DataSet run(final String sql) throws JSQLParserException
    {
        if(sql.trim().isEmpty())
            return null;

        var query = sql .trim().toLowerCase();
        var statement = CCJSqlParserUtil.parse(query);
        return run(statement);
    }

    private DataSet run(final Statement statement) {
        if (statement instanceof Select)
            return select((Select) statement);
        else if (statement instanceof CreateTable)
            createTable((CreateTable) statement);
        else if (statement instanceof Insert)
            insertInto((Insert) statement);
        else if (statement instanceof Delete)
            delete((Delete) statement);
        else if (statement instanceof Update)
            update((Update) statement);
        else if (statement instanceof Drop)
            drop((Drop) statement);
        else if (statement instanceof Alter)
            System.out.println("Alter table");
        else
            System.out.println("Não suportado!");
        return null;
    }

    private void drop(final Drop statement)
    {
        var tableName = statement
                .getName()
                .getName();
        executionEngine.dropTable(tableName);
    }

    private DataSet select(final Select statement) {
        final PlainSelect plainSelect = (PlainSelect) statement.getSelectBody();
        final List<String> tableList = new TablesNamesFinder().getTableList(statement);

        final LinkedList<String> cols = extractColumns(plainSelect);

        final Stack<Object> filters = extractFilters(plainSelect.getWhere());

        if (areThereJoins(plainSelect)){
            printJoins(tableList, plainSelect.getJoins());
            return executionEngine.getDataSet(tableList, cols, filters, plainSelect.getJoins());
        }else{
            return executionEngine.getDataSetBl(tableList, cols, filters);
        }
    }

    private LinkedList<String> extractColumns(final PlainSelect plainSelect) {
        return plainSelect.getSelectItems()
                .stream()
                .map(Object::toString)
                .collect(toCollection(LinkedList::new));
    }

    private boolean areThereJoins(final PlainSelect plainSelect) {
        return plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty();
    }

    private void printJoins(final List<String> tableList, final List<Join> joins) {
        for(int i = 0; i< joins.size(); i++){
            Join join = joins.get(i);
            System.out.println("\t-> "+ tableList.get(i)+"- "+join.toString());
        }
    }

    private void update(final Update statement) {
        System.out.println("Update");
        final String tableName = statement.getTable().getName();

        ArrayList<String> cols = new ArrayList<>();
        for (final Column col : statement.getColumns()) {
            cols.add(col.getColumnName());
        }
        final ArrayList<String> values = statement
                .getExpressions()
                .stream()
                .map(x -> Parser.removeInvalidCaracteres(x.toString()))
                .collect(toCollection(ArrayList::new));

        Stack<Object> filters = extractFilters(statement.getWhere());
        executionEngine.updateData(tableName, cols, values, filters);
    }

    private Stack<Object> extractFilters(Expression where) {
        if (where == null) return null;
        WhereStatement whereStatement = new WhereStatement();
        StringBuilder buffer = new StringBuilder();
        whereStatement.setBuffer(buffer);
        where.accept(whereStatement);
        return whereStatement.getParsedFilters();
    }

    private void delete(Delete statement) {
        System.out.println("Delete - ");
        String tableName = statement.getTable().getName();
        Stack<Object> filters = extractFilters(statement.getWhere());
        executionEngine.deleteData(tableName, filters);
    }

    private void insertInto(Insert statement) {
        List<ExpressionList> exList;
        if (statement.getItemsList() instanceof MultiExpressionList) {
            exList = ((MultiExpressionList) statement.getItemsList()).getExprList();
        } else {
            exList = new ArrayList<>();
            exList.add((ExpressionList) statement.getItemsList());
        }
        var columns = statement.getColumns();
        if(columns == null || columns.size() == 0)
            throw new UnsupportedOperationException("e necessario declarar as colunas no INSERT!");

        int s = columns.size(), j = 0;
        LinkedList<String> cols;
        cols = new LinkedList<>();
        for (int i = 0; i < s; i++) {
            cols.add(statement.getColumns().get(i).getColumnName());
        }

        for (ExpressionList e : exList) {
            List<Expression> values = e.getExpressions();
            if (valuesDifferFromColumns(statement, values)) {
                System.err.println("Problemas no insert colunas e valores diferentes");
                return;
            }
            List<String> vals = new ArrayList<>();
            for (int i = 0; i < s; i++)
            {
                Expression valueExpression = values.get(i);
                String value = removeInvalidCaracteres(valueExpression.toString());

                vals.add(value);
            }
            if (executionEngine.insertData(statement.getTable().getName(), cols, (ArrayList<String>) vals)) {
                j++;
            } else {
                System.out.println("Problemas na inserção! " + j + " linhas inseridas;");
                return;
            }
        }
    }

    private boolean valuesDifferFromColumns(Insert insert, List<Expression> values) {
        return insert.getColumns().size() != values.size();
    }

    private void createTable(CreateTable statement) {
        CreateTable ct = statement;
        List<ColumnDefinition> cl = ct.getColumnDefinitions();
        LinkedList<String> cols = new LinkedList();
        ArrayList<String> pk = new ArrayList();
        ArrayList<ForeignKey> fk = new ArrayList();
        net.sf.jsqlparser.schema.Table schemaT = ct.getTable();
        for (ColumnDefinition c : cl) {
            cols.add(c.getColumnName());
            if (c.getColumnSpecs() != null) {
                int i = 0;
                for (String s : c.getColumnSpecs()) {
                    if (s.equalsIgnoreCase("PRIMARY")) {
                        i++;
                    } else if (s.equalsIgnoreCase("KEY") && i > 0) {
                        pk.add(c.getColumnName());
                    }
                }

            }
        }
        if (ct.getIndexes() != null) {
            for (Index index : ct.getIndexes()) {
                if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                    for (String c : index.getColumnsNames()) {
                        pk.add(c);
                    }
                } else if (index.getType().equalsIgnoreCase("FOREIGN KEY")) {
                     ForeignKeyIndex new_fk = (ForeignKeyIndex) index;
                     for (int i = 0; i<new_fk.getReferencedColumnNames().size();i++ ){
                         fk.add(new ForeignKey(new_fk.getColumnsNames().get(i), new_fk.getReferencedColumnNames().get(i), new_fk.getTable().getName()));
                     }
                }
            }
        }
        Table dt;
        if (schemaT.getSchemaName() != null) {
            dt = new Table(schemaT.getName(), executionEngine.getTarget(schemaT.getSchemaName()), pk, fk, cols);
        } else {
            String tableNome = schemaT.getName();
            NoSQL targety = executionEngine.getTarget(null);
            dt = new Table(tableNome, targety, pk, fk, cols);
        }
        executionEngine.createTable(dt);
    }

    public void changeCurrentDB(String db) {
        executionEngine.changeCurrentDB(db);
    }

    public void addNoSqlTarget(NoSQL noSQL) {
        executionEngine.addTarget(noSQL);
    }

    public List<NoSQL> getNoSqlTargets() {
        return executionEngine.getTargets();
    }

    public BDR getCurrentDataBase() {
        return executionEngine.getCurrentDb();
    }

    public List<BDR> getRdbms() {
        return executionEngine.getRdbms();
    }

    public static String removeInvalidCaracteres(String base)
    {
        return base.replaceAll("^(['\"])(.*)\\1$", "$2");
    }
}
