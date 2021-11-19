/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

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

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    public Optional<DataSet> run(final String sql) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);

            return run(statement);
        } catch (JSQLParserException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
            return Optional.empty();
        }
    }

    private Optional<DataSet> run(final Statement statement) {
        try {
            if (statement instanceof Select) {
                return Optional.ofNullable(select((Select) statement));
            } else if (statement instanceof CreateTable) {
                createTable((CreateTable) statement);
                return Optional.empty();
            } else if (statement instanceof Insert) {
                insertInto((Insert) statement);
                return Optional.empty();
            } else if (statement instanceof Delete) {
                delete((Delete) statement);
                return Optional.empty();
            } else if (statement instanceof Update) {
                update((Update) statement);
                return Optional.empty();
            } else if (statement instanceof Drop) {
                System.out.println("Drop table");
            } else if (statement instanceof Alter) {
                System.out.println("Alter table");
            } else {
                System.out.println("Não suportado!");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return Optional.empty();
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
                .map(Object::toString)
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

    private boolean insertInto(Insert statement) {
        Insert insert = statement;
        List<ExpressionList> exList;
        if (insert.getItemsList() instanceof MultiExpressionList) {
            exList = ((MultiExpressionList) insert.getItemsList()).getExprList();
        } else {
            exList = new ArrayList<>();
            exList.add((ExpressionList) insert.getItemsList());
        }
        int s = insert.getColumns().size(), j = 0;
        LinkedList<String> cols;
        cols = new LinkedList<>();
        for (int i = 0; i < s; i++) {
            cols.add(insert.getColumns().get(i).getColumnName());
        }

        for (ExpressionList e : exList) {
            List<Expression> values = e.getExpressions();
            if (valuesDifferFromColumns(insert, values)) {
                System.err.println("Problemas no insert colunas e valores diferentes");
                return true;
            }
            List<String> vals = new ArrayList<>();
            for (int i = 0; i < s; i++) {
                vals.add(values.get(i).toString());
            }
            if (executionEngine.insertData(insert.getTable().getName(), cols, (ArrayList<String>) vals)) {
                j++;
            } else {
                System.out.println("Problemas na inserção! " + j + " linhas inseridas;");
                return true;
            }
        }

        System.out.println("Inserção executada com sucesso! " + j + " linhas inseridas;");
        return false;
    }

    private boolean valuesDifferFromColumns(Insert insert, List<Expression> values) {
        return insert.getColumns().size() != values.size();
    }

    private void createTable(CreateTable statement) {
        System.out.println("Create table");
        CreateTable ct = statement;
        List<ColumnDefinition> cl = ct.getColumnDefinitions();
        LinkedList<String> cols = new LinkedList();
        ArrayList<String> pk = new ArrayList();
        ArrayList<ForeignKey> fk = new ArrayList();
        net.sf.jsqlparser.schema.Table schemaT = ct.getTable();
        System.out.print("\n" + schemaT.getName() + "\n");
        for (ColumnDefinition c : cl) {
            cols.add(c.getColumnName());
            if (c.getColumnSpecStrings() != null) {
                int i = 0;
                for (String s : c.getColumnSpecStrings()) {
                    if (s.equals("PRIMARY")) {
                        i++;
                    } else if (s.equals("KEY") && i > 0) {
                        pk.add(c.getColumnName());
                    }
                }

            }
        }
        if (ct.getIndexes() != null) {
            for (Index index : ct.getIndexes()) {
                if (index.getType().equals("PRIMARY KEY")) {
                    for (String c : index.getColumnsNames()) {
                        pk.add(c);
                    }
                } else if (index.getType().equals("FOREIGN KEY")) {
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
            dt = new Table(schemaT.getName(), executionEngine.getTarget(null), pk, fk, cols);
        }
        if (executionEngine.createTable(dt)) {
            System.out.println("Tabela Criada");
        } else {
            System.out.println("Tabela não Criada");
        }
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
}
