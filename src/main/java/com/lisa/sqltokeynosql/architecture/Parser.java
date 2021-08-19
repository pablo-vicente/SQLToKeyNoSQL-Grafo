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
import net.sf.jsqlparser.statement.Statements;
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
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import util.DataSet;
import util.Dictionary;
import util.SQL.ForeignKey;
import util.SQL.Table;
import util.SQL.WhereStatement;
import util.TimeConter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author geomar
 */
public class Parser {

    private final ExecutionEngine executionEngine;
    public ArrayList<HashMap<String, String>> dataSet;
    public DataSet resultSet;
    public long timeToDO;
    public final Stack<Integer> stack;

    public Parser() {
        executionEngine = new ExecutionEngine();
        stack = new Stack<>();
        stack.push(1);
    }

    public boolean run(String sql) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            long setTime = new Date().getTime();

            boolean result = this.run(statement);
            long resetTime = new Date().getTime();
            timeToDO = resetTime - setTime;
            return result;
        } catch (JSQLParserException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    public boolean run(File script) {
        FileReader fr;
        boolean result = false;
        try {
            fr = new FileReader(script);
            BufferedReader br = new BufferedReader(fr);
            String s;
            StringBuilder sb = new StringBuilder();
                timeToDO = 0;
            while ((s = br.readLine()) != null) {
                sb.append(s);
                
                if (sb.toString().contains(";")){
                    TimeConter.current = 0;
                    long setTime = new Date().getTime();
                    long resetTime = 0;
                    Statements stmt = CCJSqlParserUtil.parseStatements(sb.toString());

                    for (Statement st : stmt.getStatements()) {
                        result &= this.run(st);
                    }
                    resetTime = new Date().getTime();
                    timeToDO += resetTime - setTime;
                    sb = new StringBuilder();
                }
            }
            br.close();
            
        } catch (Exception ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return result;
    }

    private boolean run(Statement statement) {
        dataSet = null;
        resultSet = null;
        try {
            if (statement instanceof Select) {
                select((Select) statement);
            } else if (statement instanceof CreateTable) {
                createTable((CreateTable) statement);
            } else if (statement instanceof Insert) {
                if (insertInto((Insert) statement)) return false;
            } else if (statement instanceof Delete) {
                delete((Delete) statement);
            } else if (statement instanceof Update) {
                update((Update) statement);
            } else if (statement instanceof Drop) {
                System.out.println("Drop table");
            } else if (statement instanceof Alter) {
                System.out.println("Alter table");
            } else {
                System.out.println("Não suportado!");
            }
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return false;
        }
        return true;
    }

    private void select(Select statement) {
        PlainSelect plainSelect = (PlainSelect) statement.getSelectBody();
        List<String> tableList = new TablesNamesFinder().getTableList(statement);

        LinkedList<String> cols = extractColumns(plainSelect);

        Stack<Object> filters = extractFilters(plainSelect);

        if (areThereJoins(plainSelect)){
            printJoins(tableList, plainSelect.getJoins());
            resultSet = executionEngine.getDataSet(tableList, cols, filters, plainSelect.getJoins());
        }else{
            resultSet = executionEngine.getDataSetBl(tableList, cols, filters);
        }
        if (resultSet == null) {
            System.out.println("Ocorreu algum erro!");
        } else {
            System.out.println("Foram encontras " + resultSet.getData().size() + " tuplas!\n");
        }
    }

    private LinkedList<String> extractColumns(PlainSelect plainSelect) {
        return plainSelect.getSelectItems()
                .stream()
                .map(Object::toString)
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private boolean areThereJoins(PlainSelect plainSelect) {
        return plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty();
    }

    private void printJoins(List<String> tableList, List<Join> joins) {
        for(int i = 0; i< joins.size(); i++){
            Join join = joins.get(i);
            System.out.println("\t-> "+ tableList.get(i)+"- "+join.toString());
        }
    }

    private Stack<Object> extractFilters(PlainSelect plainSelect) {
        if (plainSelect.getWhere() == null) {
            return null;
        }
        WhereStatement whereStatement = new WhereStatement();
        StringBuilder stringBuilder = new StringBuilder();
        whereStatement.setBuffer(stringBuilder);
        Expression expression = plainSelect.getWhere();
        expression.accept(whereStatement);
        return whereStatement.getParsedFilters();
    }

    private void update(Update statement) {
        System.out.println("Update");
        Update update = statement;
        String t = update.getTable().getName();

        ArrayList<String> cols = new ArrayList();
        for (Column col : update.getColumns()) {
            cols.add(col.getColumnName());
        }
        ArrayList <String> vals = new <String> ArrayList();
        for (Expression e : update.getExpressions()){
            vals.add(e.toString());
        }
        Stack<Object> filters = null;
        if (update.getWhere() != null) {
            Expression e = update.getWhere();
            ExpressionDeParser deparser = new WhereStatement();
            StringBuilder b = new StringBuilder();
            deparser.setBuffer(b);
            e.accept(deparser);
            filters = ((WhereStatement) deparser).getParsedFilters();
        }
        executionEngine.updateData(t, cols, vals, filters);
    }

    private void delete(Delete statement) {
        System.out.println("Delete - ");
        Delete del = statement;
        String t = del.getTable().getName();
        Stack<Object> filters = null;
        if (del.getWhere() != null) {
            Expression e = del.getWhere();
            ExpressionDeParser deparser = new WhereStatement();
            StringBuilder b = new StringBuilder();
            deparser.setBuffer(b);
            e.accept(deparser);
            filters = ((WhereStatement) deparser).getParsedFilters();
        }
        executionEngine.deleteData(t, filters);
    }

    private boolean insertInto(Insert statement) {
        Insert ins = statement;
        List<ExpressionList> exList;
        if (ins.getItemsList() instanceof MultiExpressionList) {
            exList = ((MultiExpressionList) ins.getItemsList()).getExprList();
        } else {
            exList = new <ExpressionList>ArrayList();
            exList.add((ExpressionList) ins.getItemsList());
        }
        int s = ins.getColumns().size(), j = 0;
        LinkedList<String> cols;
        ArrayList vals;
        cols = new <String> LinkedList();
        for (int i = 0; i < s; i++) {
            cols.add(ins.getColumns().get(i).getColumnName());
        }

        for (ExpressionList e : exList) {
            List<Expression> values = e.getExpressions();
            if (ins.getColumns().size() != values.size()) {
                System.err.println("Problemas no insert colunas e valores diferentes");
                return true;
            }
            vals = new <String> ArrayList();
            for (int i = 0; i < s; i++) {
                vals.add(values.get(i).toString());
            }
            if (executionEngine.insertData(ins.getTable().getName(), cols, vals)) {
                j++;
            } else {
                System.out.println("Problemas na inserção! " + j + " linhas inseridas;");
                return true;
            }
        }

        System.out.println("Inserção executada com sucesso! " + j + " linhas inseridas;");
        return false;
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
            dt = new Table(schemaT.getName(), executionEngine.getDictionary().getTarget(schemaT.getSchemaName()), pk, fk, cols);
        } else {
            dt = new Table(schemaT.getName(), executionEngine.getDictionary().getTarget(null), pk, fk, cols);
        }
        if (executionEngine.createTable(dt)) {
            System.out.println("Tabela Criada");
        } else {
            System.out.println("Tabela não Criada");
        }
    }

    public Dictionary getDic() {
        return executionEngine.getDictionary();
    }

    public void changeCurrentDB(String db) {
        executionEngine.changeCurrentDB(db);
    }

}
