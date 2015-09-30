/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.io.Serializable;
import java.util.ArrayList;

/**
 *
 * @author geomar
 */
public class Dictionary implements Serializable{

    private BDR current_db = null;
    private ArrayList<BDR> Bdrs;
    private ArrayList<NoSQL> targets;

    public Dictionary() {
        this.Bdrs = new <BDR> ArrayList();
        this.targets = new <NoSQL> ArrayList();
    }

    public ArrayList<BDR> getBdrs() {
        return Bdrs;
    }

    public void setBdrs(ArrayList<BDR> Bdrs) {
        this.Bdrs = Bdrs;
    }

    public ArrayList<NoSQL> getTargets() {
        return targets;
    }

    public void setTargets(ArrayList<NoSQL> targets) {
        this.targets = targets;
    }

    public BDR getRBD(String dbn) {
        if (dbn != null) {
            for (BDR db : Bdrs) {
                if (dbn.equals(db.getName())) {
                    return db;
                }
            }
        }
        return null;
    }

    public NoSQL getTarget(String alias) {
        if (alias == null) {
            return targets.get(0);
        }

        for (NoSQL t : targets) {
            if (t.getAlias().equals(alias)) {
                return t;
            }
        }
        return null;
    }

    public BDR getCurrent_db() {
        return current_db;
    }

    public void setCurrent_db(String current_db) {
        this.current_db = this.getRBD(current_db);
        for (NoSQL n: targets){
            n.getConection().connect(current_db);
        }
    }

}
