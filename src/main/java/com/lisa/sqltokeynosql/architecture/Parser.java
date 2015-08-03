/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.architecture;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
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
import util.NoSQL;
import util.Table;

/**
 *
 * @author geomar
 */
public class Parser {

    private String sql;
    private ExecutionEngine ex;

    public Parser(String sql) {
        this.sql = sql;
        ex = new ExecutionEngine();
        ex.createDBR("teste");
    }

    public boolean run() {

        try {

            Statement statement = CCJSqlParserUtil.parse(sql);
            
            if (statement instanceof Select) {
                System.out.println("Select");
                Select selectStatement = (Select) statement;
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
                String tab = "Tabelas: ";
                for (String t : tableList) {
                    tab += t + ", ";
                }
                System.out.println("\n ak:");
                PlainSelect ps = (PlainSelect) selectStatement.getSelectBody();
                System.out.println("Itens + Tabela");
                for (SelectItem si : ps.getSelectItems()) {
                    System.out.println(si.toString());
                }

                System.out.println("Tabelas " + ps.getFromItem().toString());

                System.out.print(tab + "\n-------");
            } else if (statement instanceof CreateTable) {
                System.out.println("Create table");
                CreateTable ct = (CreateTable) statement;
                List<ColumnDefinition> cl =  ct.getColumnDefinitions();
                ArrayList <String> cols = new ArrayList();
                ArrayList <String> pk = new ArrayList();
                ArrayList <String> fk = new ArrayList();
       net.sf.jsqlparser.schema.Table schemaT = ct.getTable();
                System.out.print("\n"+schemaT.getName()+"\n");
                for (ColumnDefinition c : cl){
                    cols.add(c.getColumnName());
                    if (c.getColumnSpecStrings()!= null){
                        int i=0;
                        for( String s: c.getColumnSpecStrings()){
                            if (s.equals("PRIMARY"))
                                i++;
                            else if (s.equals("FOREIGN KEY") && i>0)
                                System.out.println("Chave Estrangeira!");
                        }
                        
                    }
                    //System.out.print("COluna:"+c.getColumnName()+", t: "+c.getColDataType().toString()+" \n");
                }
                if (ct.getIndexes() != null){
                    for (Index index : ct.getIndexes()){
                        if (index.getType().equals("PRIMARY KEY")){
                            for (String c : index.getColumnsNames())
                                pk.add(c);
                        }else if (index.getType().equals("PRIMARY KEY")){
                            
                        }
                        //System.out.print("n: "+index.getName()+", tipo: "+index.getType()+ ", col: "+ index.getColumnsNames()+" o:"+index.toString());
                    }
                }
                Table dt = new Table(schemaT.getName(), new NoSQL("teste", "user", "senha", "endereço"), pk, null, cols);
                ex.createTable(dt);
                
            } else if (statement instanceof Insert) {
                System.out.println("Insert");
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
            System.out.print("\n\nDigite um comando:\n\n>");
        } catch (JSQLParserException ex) {
            System.err.println(ex.getMessage());
            return false;
        }

        return true;
    }

}
