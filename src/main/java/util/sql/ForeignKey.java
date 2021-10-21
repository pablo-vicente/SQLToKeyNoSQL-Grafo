package util.sql;

/**
 *
 * @author geomar
 */
public class ForeignKey {
    private String att;
    private String rAtt;
    private String rTable;

    public ForeignKey(String att, String rAtt, String rTable) {
        this.att = att;
        this.rAtt = rAtt;
        this.rTable = rTable;
    }    
    
    public String getAtt() {
        return att;
    }

    public void setAtt(String att) {
        this.att = att;
    }

    public String getrAtt() {
        return rAtt;
    }

    public void setrAtt(String rAtt) {
        this.rAtt = rAtt;
    }

    public String getrTable() {
        return rTable;
    }

    public void setrTable(String rTable) {
        this.rTable = rTable;
    }
    
    

}
