/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lisa.sqltokeynosql.util;

import com.lisa.sqltokeynosql.util.sql.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author geomar
 */
public final class Dictionary implements Serializable {

    private static final int DB = 0;
    private static final int TABLE = 1;
    private BDR currentDb = null;
    private List<BDR> rdbms;
    private List<NoSQL> targets;

    public Dictionary() {
        this.rdbms = new ArrayList<>();
        this.targets = new ArrayList<>();
    }

    public List<BDR> getRdbms() {
        return rdbms;
    }

    public List<NoSQL> getTargets() {
        return targets;
    }

    public void addTarget(NoSQL noSQL) {
        targets.add(noSQL);
    }

    public Optional<BDR> getBDR(String dbName) {

        for (BDR db : rdbms) {
            if (dbName.equalsIgnoreCase(db.getName())) {
                return Optional.of(db);
            }
        }

        return Optional.empty();
    }

    public NoSQL getTarget(String conector)
    {
        if(targets == null || targets.size() == 0 || conector == null)
            throw new UnsupportedOperationException("Não foi cadastrado nenhum NoSQL target.");

        var conectorNoSql = com.lisa.sqltokeynosql.api.enums.Connector.valueOf(conector.toUpperCase());
        for (NoSQL noSQL : targets)
        {
            if (noSQL.getConnector() == conectorNoSql) {
                return noSQL;
            }
        }
        throw new UnsupportedOperationException("Não foi cadastrado nenhum NoSQL target para " + conector);
    }

    public BDR getCurrentDb() {
        return currentDb;
    }

    public void setCurrentDb(String currentDb)
    {
        getBDR(currentDb).ifPresent(dbr -> this.currentDb = dbr);
        this.currentDb.getTargetDB().getConnection().connect(currentDb);
    }

    public Optional<Table> getTable(String tableName) {
        if (tableSpecifiesDB(tableName)) {
            return getTableFromDB(tableName);
        }

        return this.currentDb.getTable(tableName);
    }

    private Optional<Table> getTableFromDB(String tableReference) {
        String[] split = tableReference.split("\\.");
        String dbName = split[DB];
        String tableName = split[TABLE];
        return getBDR(dbName)
                .flatMap(dbr -> dbr.getTable(tableName));
    }

    private static boolean tableSpecifiesDB(String tableName) {
        return tableName.contains(".");
    }

}
