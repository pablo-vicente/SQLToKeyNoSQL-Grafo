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
        BDR bdr = new BDR(noSQL.getAlias(), new ArrayList<>());
        rdbms.add(bdr);
        if (currentDb == null)
            currentDb = bdr;
    }

    public Optional<BDR> getBDR(String dbName) {

        for (BDR db : rdbms) {
            if (dbName.equals(db.getName())) {
                return Optional.of(db);
            }
        }

        return Optional.empty();
    }

    public NoSQL getTarget(String alias) {
        if (alias == null) {
            return targets.get(0);
        }

        for (NoSQL noSQL : targets) {
            if (noSQL.getAlias().equals(alias)) {
                return noSQL;
            }
        }
        return null;
    }

    public BDR getCurrentDb() {
        return currentDb;
    }

    public void setCurrentDb(String currentDb) {
        getBDR(currentDb).ifPresent(dbr -> this.currentDb = dbr);
        targets.forEach(noSQL -> noSQL.getConnection().connect(currentDb));
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
                .flatMap(dbr -> getTable(tableName));
    }

    private static boolean tableSpecifiesDB(String tableName) {
        return tableName.contains(".");
    }

}
