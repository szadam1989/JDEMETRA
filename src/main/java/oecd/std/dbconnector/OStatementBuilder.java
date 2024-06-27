/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oecd.std.dbconnector;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import ec.tss.tsproviders.db.DbSetId;
import ec.tstoolkit.design.IBuilder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import static javax.swing.JOptionPane.showMessageDialog;
import org.openide.util.Exceptions;
import static oecd.std.dbconnector.OJdbcAccessor.CALLTYPE;
import static oecd.std.dbconnector.OJdbcAccessor.CALLTYPE.*;
import org.slf4j.Logger;

/**
 *
 * @author Gyomai_G
 * It builds main Prepared Statements required by the DBAccessor. It hosts two private, self-contained validation accessors.
 * It is functionally similar to the select-builder from the view/table-only accessor.
 */
public class OStatementBuilder implements IBuilder<PreparedStatement> {
     
    private final CALLTYPE callType;
    private final ODbBean obean;
    private final DbSetId browsingContext;
    private final Connection conn;
    private String sqlstring;
    private String columnNames;
    private String limitDomains;
    private String cleanLimitDomains;
    private ListMultimap<String,String> limitMap;
    protected Logger logger;
    
    
    private PreparedStatement pstmt;
            
 public OStatementBuilder(CALLTYPE callType, ODbBean obean, DbSetId browsingContext, Connection conn, Logger logger) {
       this.obean = obean;
       this.callType = callType;
       this.browsingContext = browsingContext;
       this.conn = conn;
       this.logger = logger;
       this.sqlstring = "{call SPNAME(?,?,?)}"; //callType, column, constraints as parameters
            this.sqlstring = this.sqlstring.replaceFirst("SPNAME", obean.getTableName());
       this.columnNames="";
       this.limitDomains="";
       this.cleanLimitDomains="";
       this.limitMap = ArrayListMultimap.create();
 }
 
 public String getSQLString(){
     return sqlstring;
 }
 
 public ListMultimap<String,String> constraintMap() {
     return limitMap;
 }
 
 public String sConstraintMap() {

     //initialize domain constraints
    limitDomains = obean.getFilterParam(); 
    String[] constraints = limitDomains.split("\\s*,+\\s*");
    for (String cons: constraints){
        String[] keyvaluepair = cons.split("\\s*=+\\s*");
        limitMap.put(keyvaluepair[0],keyvaluepair[1]);
    }
                
     // add context constraints;
    for (int i = 0; i < browsingContext.getLevel(); i++) {
        if (limitMap.containsKey(browsingContext.getColumn(i))){
            //context constraints are AND-type constraints, hence all potential OR-constraints are removed  
            limitMap.removeAll(browsingContext.getColumn(i));
        }
        limitMap.put(browsingContext.getColumn(i), browsingContext.getValue(i));
    }
            
     // do validation - with roundtrips to the database; drop invalid keys and values
    List<String> validColumnNames = this.getValidColumnNames();
    for (String key : limitMap.keySet()) {
        if (validColumnNames.contains(key)){
        // List<String> validElements = this.getValidElements(key);    //ELEMENT VALIDATION SWITCHED OFF TO SPEED the process
            for (ListIterator<String> it = limitMap.get(key).listIterator(); it.hasNext();) {
                String elem = it.next();
            //    if (validElements.contains(elem)){
                    cleanLimitDomains += key + "=" + elem +";";
            //    }
            }
        }
        else {
            //limitMap.removeAll(key);
        }
    } 
    return cleanLimitDomains;
 }//Függvény kommentben volt
 
 private List<String> getValidColumnNames(){
    List<String> colList = new ArrayList<>();
    try {
        PreparedStatement auxps = conn.prepareStatement(sqlstring);
        auxps.setString(1, CALLTYPE.ColumnValidation.name());
        auxps.setString(2, null);
        auxps.setString(3, null);
            logger.info(sqlstring + "; " + CALLTYPE.ColumnValidation.name());
        ResultSet rs = auxps.executeQuery();
        while (rs.next()) {              
            colList.add(rs.getString(1));
        }
    } catch (SQLException ex){
        Exceptions.printStackTrace(ex);
    }
    return colList;
 }
 
 private List<String> getValidElements(String columnName) {
    List<String> elemList = new ArrayList<>();
    try {
        PreparedStatement auxps = conn.prepareStatement(sqlstring);
        auxps.setString(1, CALLTYPE.ElementValidation.name());
        auxps.setString(2, columnName);
        auxps.setString(3, null);
            logger.info(sqlstring + "; " + CALLTYPE.ElementValidation.name() + "; " +  columnName);        
        ResultSet rs = auxps.executeQuery();
        while (rs.next()) {              
            elemList.add(rs.getString(1));
        }
    } catch (SQLException ex){
        Exceptions.printStackTrace(ex);
    }
    
    return elemList;   
    }
 
 @Override
 public PreparedStatement build(){
     Joiner joiner = Joiner.on(",").skipNulls();
     String versionColumn = obean.getVersionColumn();
     if (versionColumn.isEmpty()) {
         versionColumn = null;
     } 
         
         
     switch (callType) {
         case Children: 
             columnNames = browsingContext.getColumn(browsingContext.getLevel());
         break;
         case AllSeriesWithData:
             columnNames = joiner.join(browsingContext.selectColumns());
             columnNames = joiner.join(columnNames,obean.getPeriodColumn(),versionColumn,obean.getValueColumn());
         break;       
         case AllSeries:
             columnNames = obean.getDimColumns();
             break;
         case SeriesWithData:
              columnNames = joiner.join(obean.getPeriodColumn(),versionColumn,obean.getValueColumn());
             break;
         default:
                throw new AssertionError(callType.name());
        }
        
    try {
            pstmt = conn.prepareStatement(sqlstring);
            pstmt.setString(1, callType.name());
            pstmt.setString(2, columnNames);
            pstmt.setString(3, this.sConstraintMap());
                logger.info(sqlstring + "; " + callType.name() + "; " +  columnNames + "; " + cleanLimitDomains);   
                
        } catch (SQLException ex) {
            Exceptions.printStackTrace(ex);
        }
    return pstmt;
 }//Függvény kommentben volt

  /*  @Override
    public PreparedStatement build() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }*/
}