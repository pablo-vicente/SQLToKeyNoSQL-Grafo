/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import util.DataSet;
import util.NoSQL;
import util.Table;
import util.WhereStatment;

/**
 *
 * @author geomar
 */
public class Parser {

    private ExecutionEngine ex;
    public ArrayList<HashMap<String, String>> dataSet;
    public DataSet ds;

    public Parser() {
        ex = new ExecutionEngine();
        ex.createDBR("teste");
    }

    public boolean run(String sql) {
        dataSet = null;
        ds = null;
        try {

            Statement statement = CCJSqlParserUtil.parse(sql);

            if (statement instanceof Select) {
                System.out.println("Select");
                Select selectStatement = (Select) statement;
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
                ArrayList<String> cols = new ArrayList();
                PlainSelect ps = (PlainSelect) selectStatement.getSelectBody();
                for (SelectItem si : ps.getSelectItems()) {
                    cols.add(si.toString());
                }
                ArrayList<Object> filters = null;// = new <String>ArrayList();
                Expression e = ps.getWhere();

                //for (Expression s : ps.getWhere()) {
                //  cols.add(si.toString());
                //}
                System.out.println("-> " + ps.getWhere());
                if (ps.getWhere() != null) {
                    WhereStatment ws = new WhereStatment();
                    ps.getWhere().accept(ws);
                    filters = ws.getParsedFilters();
                }
                
                //dataSet = ex.getData(tableList, cols, null);

                ds = ex.getDataSet(tableList, cols, filters);;
                /* ds.setColumns(cols);
                 for (HashMap<String, String> a: dataSet){
                 String []v = new String[cols.size()];
                 for (int i=0; i<cols.size();i++){
                 v[i] = a.get(cols.get(i));
                 }
                 ds.getData().add(v);
                 }
                 */
                if (ds == null) {
                    System.out.println("Ocorreu algum erro!");
                } else {
                    System.out.println("Foram encontras " + ds.getData().size() + " tuplas!\n");
                }

            } else if (statement instanceof CreateTable) {
                System.out.println("Create table");
                CreateTable ct = (CreateTable) statement;
                List<ColumnDefinition> cl = ct.getColumnDefinitions();
                ArrayList<String> cols = new ArrayList();
                ArrayList<String> pk = new ArrayList();
                ArrayList<String> fk = new ArrayList();
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
                    //System.out.print("COluna:"+c.getColumnName()+", t: "+c.getColDataType().toString()+" \n");
                }
                if (ct.getIndexes() != null) {
                    for (Index index : ct.getIndexes()) {
                        if (index.getType().equals("PRIMARY KEY")) {
                            for (String c : index.getColumnsNames()) {
                                pk.add(c);
                            }
                        } else if (index.getType().equals("PRIMARY KEY")) {

                        }
                        //System.out.print("n: "+index.getName()+", tipo: "+index.getType()+ ", col: "+ index.getColumnsNames()+" o:"+index.toString());
                    }
                }
                Table dt = new Table(schemaT.getName(), new NoSQL("teste", "user", "senha", "endereço"), pk, null, cols);
                if (ex.createTable(dt)) {
                    System.out.println("Tabela Criada");
                } else {
                    System.out.println("Tabela não Criada");
                }

            } else if (statement instanceof Insert) {
                Insert ins = (Insert) statement;
                List<Expression> values = ((ExpressionList) ins.getItemsList()).getExpressions();
                //System.out.println("Insert - "+ins.getTable().getName());
                if (ins.getColumns().size() != values.size()) {
                    System.err.println("Problemas no insert colunas e valores diferentes");
                    return false;
                }
                int s = ins.getColumns().size();
                ArrayList<String> cols, vals;
                vals = new <String> ArrayList();
                cols = new <String> ArrayList();
                for (int i = 0; i < s; i++) {
                    cols.add(ins.getColumns().get(i).getColumnName());
                    vals.add(values.get(i).toString());
                }

                if (ex.insertData(ins.getTable().getName(), cols, vals)) {
                    System.out.println("Inserção executada com sucesso!");
                } else {
                    System.out.println("Problemas na inserção!");
                }

            } else if (statement instanceof Delete) {
                System.out.println("Delete");
            } else if (statement instanceof Update) {
                System.out.println("Update");
            } else if (statement instanceof Drop) {
                System.out.println("Drop table");
            } else if (statement instanceof Alter) {
                System.out.println("Alter table");
            } else {
                System.out.println("Não suportado!");
            }
        } catch (JSQLParserException ex) {
            System.err.println(ex.getMessage());
            return false;
        }

        return true;
    }

}
