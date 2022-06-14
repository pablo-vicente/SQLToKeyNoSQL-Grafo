/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
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
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import util.DataSet;
import util.Dictionary;
import util.SQL.ForeignKey;
import util.NoSQL;
import util.SQL.Table;
import util.TimeConter;
import util.SQL.WhereStatment;

/**
 *
 * @author geomar
 */
public class Parser {

    private ExecutionEngine ex;
    public ArrayList<HashMap<String, String>> dataSet;
    public DataSet ds;
    public long timeToDO;
    public final Stack<Integer> stak;

    public Parser() {
        ex = new ExecutionEngine();
        //ex.createDBR("teste");
        stak = new Stack<>();
        stak.push(1);
    }

    public boolean run(String sql) {
        Statement statement;
        boolean result;
        try {
            statement = CCJSqlParserUtil.parse(sql);
            timeToDO = 0;
            TimeConter.current = 0;
            long setTime = new Date().getTime();
            long resetTime = 0;

            result = this.run(statement);
            resetTime = new Date().getTime();
            timeToDO = resetTime - setTime;
        } catch (JSQLParserException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return result;
    }

    public boolean run(File script) {
        FileReader fr;
        Boolean result = false;
        try {
            fr = new FileReader(script);
            // be sure to not have line starting with "--" or "/*" or any other non aplhabetical character

            BufferedReader br = new BufferedReader(fr);
            String s;
            StringBuffer sb = new StringBuffer();
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
                    sb = new StringBuffer();
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
        ds = null;
        try {
            if (statement instanceof Select) {
                Select selectStatement = (Select) statement;
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
                
                LinkedList<String> cols = new LinkedList<>();
                PlainSelect ps = (PlainSelect) selectStatement.getSelectBody();
                for (SelectItem si : ps.getSelectItems()) {
                    cols.add(si.toString());
                }
                //ps.getFromItem().
                Stack<Object> filters = null;
                if (ps.getWhere() != null) {
                    Expression e = ps.getWhere();
                    ExpressionDeParser deparser = new WhereStatment();
                    StringBuilder b = new StringBuilder();
                    deparser.setBuffer(b);
                    e.accept(deparser);
                    filters = ((WhereStatment) deparser).getParsedFilters();
                }
                
                if (ps.getJoins() != null && !ps.getJoins().isEmpty()){
                  List <Join> joins = ps.getJoins();
                  for(int i = 0; i< joins.size();i++){
                      Join j = joins.get(i);
                      System.out.println("\t-> "+tableList.get(i)+"- "+j.toString());
                  }
                  ds = ex.getDataSet(tableList, cols, filters, joins);
                }else{
                    ds = ex.getDataSetBl(tableList, cols, filters);
                }
                if (ds == null) {
                    System.out.println("Ocorreu algum erro!");
                } else {
                    System.out.println("Foram encontras " + ds.getData().size() + " tuplas!\n");
                }

            } else if (statement instanceof CreateTable) {
                System.out.println("Create table");
                CreateTable ct = (CreateTable) statement;
                List<ColumnDefinition> cl = ct.getColumnDefinitions();
                LinkedList<String> cols = new LinkedList();
                ArrayList<String> pk = new ArrayList();
                ArrayList<ForeignKey> fk = new ArrayList();
                net.sf.jsqlparser.schema.Table schemaT = ct.getTable();
                System.out.print("\n" + schemaT.getName() + "\n");
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
                    //System.out.print("COluna:"+c.getColumnName()+", t: "+c.getColDataType().toString()+" \n");
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
                        //System.out.print("n: "+index.getName()+", tipo: "+index.getType()+ ", col: "+ index.getColumnsNames()+" o:"+index.toString());
                    }
                }
                Table dt;
                //String [] table = (schemaT.getName()).split(".");
                if (schemaT.getSchemaName() != null) {
                    dt = new Table(schemaT.getName(), ex.getDic().getTarget(schemaT.getSchemaName()), pk, fk, cols);
                } else {
                    String tableNome = schemaT.getName();
                    Dictionary dictionary = ex.getDic();
                    NoSQL targety = dictionary.getTarget(null);

                    dt = new Table(tableNome, targety, pk, fk, cols);
                }
                if (ex.createTable(dt)) {
                    System.out.println("Tabela Criada");
                } else {
                    System.out.println("Tabela não Criada");
                }

            } else if (statement instanceof Insert) {
                Insert ins = (Insert) statement;
                List<ExpressionList> exList;
                if (ins.getItemsList() instanceof MultiExpressionList) {
                    exList = ((MultiExpressionList) ins.getItemsList()).getExprList();
                } else {
                    exList = new <ExpressionList>ArrayList();
                    exList.add((ExpressionList) ins.getItemsList());
                }

                List<Column> columns = ins.getColumns();
                if(columns == null || columns.size() == 0)
                    throw new UnsupportedOperationException("e necessario declarar as colunas no INSERT!");

                int s = columns.size(), j = 0;
                LinkedList<String> cols;
                        ArrayList vals;
                cols = new <String> LinkedList();
                for (int i = 0; i < s; i++) {
                    cols.add(ins.getColumns().get(i).getColumnName());
                }
                // long setTime = new Date().getTime();
                // long resetTime = 0;

                for (ExpressionList e : exList) {
                    List<Expression> values = e.getExpressions();
                    if (ins.getColumns().size() != values.size()) {
                        System.err.println("Problemas no insert colunas e valores diferentes");
                        return false;
                    }
                    vals = new <String> ArrayList();
                    for (int i = 0; i < s; i++)
                    {
                        Expression valueExpression = values.get(i);
                        String value = valueExpression
                                .toString()
                                .replaceAll("^(['\"])(.*)\\1$", "$2");

                        vals.add(value);
                    }
                    net.sf.jsqlparser.schema.Table table = ins.getTable();
                    String tableName = table.getName();

                    if (ex.insertData(tableName, cols, vals)) {
                        j++;
                    } else {
                        System.out.println("Problemas na inserção! " + j + " linhas inseridas;");
                        return false;
                    }
                }

                System.out.println("Inserção executada com sucesso! " + j + " linhas inseridas;");

            } else if (statement instanceof Delete) {
                System.out.println("Delete - ");
                Delete del = (Delete) statement;
                String t = del.getTable().getName();
                Stack<Object> filters = null;
                if (del.getWhere() != null) {
                    Expression e = del.getWhere();
                    ExpressionDeParser deparser = new WhereStatment();
                    StringBuilder b = new StringBuilder();
                    deparser.setBuffer(b);
                    e.accept(deparser);
                    filters = ((WhereStatment) deparser).getParsedFilters();
                }
                ex.deleteData(t, filters);
            } else if (statement instanceof Update) {
                System.out.println("Update");
                 Update update = (Update) statement;
                String t = update.getTable().getName();
                
                ArrayList<String> cols = new ArrayList();
                //PlainSelect ps = (PlainSelect) update.getWhere();
                for (Column col : update.getColumns()) {
                    cols.add(col.getColumnName());
                }
                ArrayList <String> vals = new <String> ArrayList();
                for (Expression e : update.getExpressions()){
                    vals.add(e.toString());
                }
                //ps.getFromItem().
                Stack<Object> filters = null;
                if (update.getWhere() != null) {
                    Expression e = update.getWhere();
                    ExpressionDeParser deparser = new WhereStatment();
                    StringBuilder b = new StringBuilder();
                    deparser.setBuffer(b);
                    e.accept(deparser);
                    filters = ((WhereStatment) deparser).getParsedFilters();
                }
                ex.updateData(t, cols, vals, filters);
                
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

    public Dictionary getDic() {
        return ex.getDic();
    }

    public void changeCurrentDB(String db) {
        ex.changeCurrentDB(db);
    }

}
