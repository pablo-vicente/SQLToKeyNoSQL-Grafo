package com.lisa.sqltokeynosql;

import api.controllers.SqlToKeyNoSqlController;

/**
 * @author geomar
 */
public class Main {
    public static void main(String[] args) {
        new SqlToKeyNoSqlController()
                .listen("localhost", 8080)
                .start();
    }
}
