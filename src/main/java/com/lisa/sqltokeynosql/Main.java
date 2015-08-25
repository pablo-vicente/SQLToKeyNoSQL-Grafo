/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql;

import com.lisa.sqltokeynosql.architecture.Parser;
import views.ViewMain;

/**
 *
 * @author geomar
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println("Nem chegou");
        // TODO code application logic here
        
        ViewMain i = new ViewMain();
        i.setVisible(true);
        
        //Parser p = new Parser("CREATE TABLE employees\n" +
//"( employee_number number(10) NOT NULL PRIMARY KEY,\n" +
//"  employee_name varchar2(50) NOT NULL PRIMARY KEY,\n" +
//"  department_id number(10),\n" +
//"  salary number(6),\n" +
//"  CONSTRAINT fk_departments\n" +
//"    FOREIGN KEY (department_id)\n" +
//"    REFERENCES departments(id)\n" +
//");");
  //      p.run();
    }
    
}
