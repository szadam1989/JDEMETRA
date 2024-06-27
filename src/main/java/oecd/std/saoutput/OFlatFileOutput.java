/*
 * Copyright 2013 National Bank of Belgium
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they will be approved 
 * by the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 * http://ec.europa.eu/idabc/eupl
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and 
 * limitations under the Licence.
 */
package oecd.std.saoutput;

import com.google.common.base.Optional;
import ec.satoolkit.ISaSpecification;
import ec.tss.sa.documents.SaDocument;
import ec.tss.sa.output.BasicConfiguration;
import ec.tss.tsproviders.utils.MultiLineNameUtil;
import ec.tstoolkit.algorithm.IOutput;
import ec.tstoolkit.timeseries.simplets.TsData;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import org.openide.util.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//Szilágyi Ádám
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import static javax.swing.JOptionPane.showMessageDialog;
import oecd.std.dbconnector.ODbBean;
import org.netbeans.api.db.explorer.ConnectionManager;
import org.netbeans.api.db.explorer.DatabaseConnection;
import org.netbeans.api.db.explorer.JDBCDriver;

/**
 *
 * @author Kristof Bayens
 * @author Gyorgy Gyomai
 */
public class OFlatFileOutput extends BasicConfiguration implements IOutput<SaDocument<ISaSpecification>> {

    public static final Logger LOGGER = LoggerFactory.getLogger(OFlatFileOutputFactory.class);
    private OFlatFileConfiguration config_;
    private OutputStreamWriter w;
    
    //Szilágyi Ádám
    Connection connection = null;
    Statement stmt = null;
    
    public static List<String> TEVList = new ArrayList<>();
    public static List<String> MHOList = new ArrayList<>();
    public static List<String> NAMEList = new ArrayList<>();
    public static List<String> TYPEList = new ArrayList<>();
    public static List<Double> VALUEList = new ArrayList<>();

    public OFlatFileOutput(OFlatFileConfiguration config) {
        config_ = config.clone();
    }

    @Override
    public void process(SaDocument<ISaSpecification> document) throws Exception {
        if (document.getResults() == null) {
            return;
        }

        String sname = document.getInput().getName();
        sname = MultiLineNameUtil.last(sname);
       /* if (config_.isFullName()) {
            sname = MultiLineNameUtil.join(sname, " * ");
        } else {
            sname = MultiLineNameUtil.last(sname);
        }*/
        sname = sname.replaceAll(",\\s*", "-");
        sname = sname.replaceAll("[\\s*]\\[frozen\\]", "");
        
        //adatbázis tábla esetén
     /*   if(config_.isFullName() == true && !sname.contains("*")){
            
            sname = config_.getFullSeriesName() + " * " + sname;
            
        }*/
        
        for (String item : config_.getSeries()) {
            TsData s = document.getResults().getData(item, TsData.class);
            if (s != null) {
             // write(w,s,sname +"-" + item);
                write(w, s, sname, item, config_.getDatabaseTableName());
            }
            
        }

    //    document.dispose();
    //    document.clear();
        
    }
    

    @Override
    public void start(Object context) {
        
    }

    @Override
    public void end(Object file) throws IOException, SQLException {
        Optional<DatabaseConnection> optDBConn = Optional.absent();
        for (DatabaseConnection c : ConnectionManager.getDefault().getConnections()) {
            optDBConn = Optional.of(c);
        }
    
        String sql = null;
        String versionFilterValue = null;
        String repairingValue = null;
        int count = 0;
        String AHO = "01";
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
        LocalDateTime now = LocalDateTime.now();   
        Statement statement = null;
        
        
        try{
            DatabaseConnection dataconn = optDBConn.get();
            Connection conn = dataconn.getJDBCConnection();
                 //     showMessageDialog(null, TEVList.size());
            int biggest = 0;
            String month = null;
            for(int i = 0; i < MHOList.size(); i++){

                if(biggest < Integer.parseInt(MHOList.get(i))){

                    biggest = Integer.parseInt(MHOList.get(i));

                }

            }

            if(biggest == 4 && Integer.parseInt(MHOList.get(MHOList.size() - 1)) == 1){
                
                AHO = "01";
                
            }else if(biggest == 4 && Integer.parseInt(MHOList.get(MHOList.size() - 1)) == 2){
                
                AHO = "04";
                
            }else if(biggest == 4 && Integer.parseInt(MHOList.get(MHOList.size() - 1)) == 3){
                
                AHO = "07";
                
            }else if(biggest == 4 && Integer.parseInt(MHOList.get(MHOList.size() - 1)) == 4){
                
                AHO = "10";
                
            }
            
            if(biggest == 12 && Integer.parseInt(MHOList.get(MHOList.size() - 1)) < 10){

                AHO = "0" + MHOList.get(MHOList.size() - 1);

            }else if(biggest == 12 && Integer.parseInt(MHOList.get(MHOList.size() - 1)) > 9){

                AHO = MHOList.get(MHOList.size() - 1);

            }
            
            statement = conn.createStatement();
           /* ResultSet versionFilter = statement.executeQuery("select ValueofVersionColumn from YI_J.INPUT_LOG where SelectDateTime = (select max(SelectDateTime) from YI_J.INPUT_LOG where user_id = '" + System.getProperty("user.name") + "')");
            
            while (versionFilter.next()) {
                versionFilterValue = versionFilter.getString("ValueofVersionColumn");
            //    showMessageDialog(null, versionFilterValue);

            }*/
          
            versionFilterValue = config_.getYI36();
            
            /*ResultSet repairing = statement.executeQuery("select distinct YI36, YI37 from " + config_.getDatabaseTableName() + " where YI36 = '" + versionFilterValue + "' and AEV = '" + TEVList.get(TEVList.size() - 1) + "' and AHO = '" + AHO + "' order by YI37");
            count = 0;
            while (repairing.next()) {
                ++count;

            }*/

            //repairingValue = String.valueOf(count);
            repairingValue = config_.getYI37();
            if (Integer.valueOf(versionFilterValue) >= 0 && Integer.valueOf(versionFilterValue) <= 69){
          //  if(count <= 6){
          
                if (Integer.valueOf(repairingValue) >= 0 && Integer.valueOf(repairingValue) <= 6){
                
                    for(int i = 0;i < TEVList.size();i++){

                        if(biggest == 4 && Integer.parseInt(MHOList.get(i)) == 1){

                            month = "01";

                        }else if(biggest == 4 && Integer.parseInt(MHOList.get(i)) == 2){

                            month = "04";

                        }else if(biggest == 4 && Integer.parseInt(MHOList.get(i)) == 3){

                            month = "07";

                        }else if(biggest == 4 && Integer.parseInt(MHOList.get(i)) == 4){

                            month = "10";

                        }

                        if(biggest == 12 && Integer.parseInt(MHOList.get(i)) < 10){

                            month = "0" + MHOList.get(i);

                        }else if(biggest == 12 && Integer.parseInt(MHOList.get(i)) > 9){

                            month = MHOList.get(i);

                        }

                        sql = "INSERT INTO " + config_.getDatabaseTableName() + "(TEV, MHO, YI35, YI38, YIJA004, YI36, YIJA003, AEV, AHO, YI37) " +
                                     "VALUES ('" + TEVList.get(i) + "', '" + month + "', '" + NAMEList.get(i) + "', '" + TYPEList.get(i) + "', " + VALUEList.get(i) + ", '" + versionFilterValue + "', TO_DATE('" + dtf.format(now) + "', 'YYYY/MM/DD HH24:MI:SS'), '" + TEVList.get(TEVList.size() - 1) + "', '" + AHO + "', '" + repairingValue + "')";

                        statement.executeUpdate(sql);
                    }

                    showMessageDialog(null, "A " + config_.getDatabaseTableName() + " tábla adatokkal történő feltöltése sikeresen megtörtént. A becslés (YI36) sorszáma: " + versionFilterValue + " A javítás (YI37) sorszáma: " + repairingValue);
                
                }else{
                    
                    showMessageDialog(null, "A javítás (YI37) sorszámának 0 és 6 közé kell esnie!");
                    
                }
            }else{
                
                showMessageDialog(null, "A becslés (YI36) sorszámának 0 és 69 közé kell esnie!");
                
            }
             /*   sql = "INSERT INTO YI_J.OUTPUT_LOG(USER_ID, TableName, YI36, YI37, AEV, AHO, InsertDateTime, Event) VALUES('" + 
                        System.getProperty("user.name") + "', '" + config_.getDatabaseTableName() + "', '" + versionFilterValue + "', '" + 
                        repairingValue + "', '" + TEVList.get(TEVList.size() - 1) + "', '" + AHO +  
                        "', TO_DATE('" + dtf.format(now) + "', 'YYYY/MM/DD HH24:MI:SS'), 'SUCCESS')";

                statement.executeUpdate(sql);*/
          /*  }else{
                
                showMessageDialog(null, "A " + config_.getDatabaseTableName() + " tábla nem került feltöltésre adatokkal, mert ezen becslésnek (YI36) már megtörtént a 6. javítása (YI37).");
                sql = "INSERT INTO YI_J.OUTPUT_LOG(USER_ID, TableName, YI36, YI37, AEV, AHO, InsertDateTime, Event) VALUES('" + 
                        System.getProperty("user.name") + "', '" + config_.getDatabaseTableName() + "', '" + versionFilterValue + "', '" + 
                        repairingValue + "', '" + TEVList.get(TEVList.size() - 1) + "', '" + AHO + 
                        "', TO_DATE('" + dtf.format(now) + "', 'YYYY/MM/DD HH24:MI:SS'), 'FAILURE')";

                statement.executeUpdate(sql);
            }*/
          
           
        }catch(NullPointerException ne){
            Exceptions.printStackTrace(ne);
            showMessageDialog(null, ne);
            showMessageDialog(null, "Nincs megnyitva az adatbáziskapcsolat!");
        }
        catch (Exception ex) {
            Exceptions.printStackTrace(ex);
            showMessageDialog(null, ex);
            
            if(sql != null){
                showMessageDialog(null, sql);
            }
         /*   showMessageDialog(null, "A program futtatása során hibajelzés következett be, amely a jobb alsó sarokban olvasható. A művelet végrehajtása sikertelen!");
            sql = "INSERT INTO YI_J.OUTPUT_LOG(USER_ID, TableName, YI36, YI37, AEV, AHO, InsertDateTime, Event, SQL_Code) VALUES('" + 
                        System.getProperty("user.name") + "', '" + config_.getDatabaseTableName() + "', '" + versionFilterValue + "', '" + 
                        repairingValue + "', '" + TEVList.get(TEVList.size() - 1) + "', '" + AHO + 
                        "', TO_DATE('" + dtf.format(now) + "', 'YYYY/MM/DD HH24:MI:SS'), 'FAILURE', '" + sql + "')";

            statement.executeUpdate(sql);
            showMessageDialog(null, sql);
            Exceptions.printStackTrace(ex);*/
        }
        
        
        TEVList.clear();
        MHOList.clear();
        TYPEList.clear();
        NAMEList.clear();
        VALUEList.clear();
    }

    @Override
    public String getName() {
        return "csv";
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    public static void write(OutputStreamWriter writeTo, TsData series, String seriesName, String seriesNameType, String dbTableNameOutput) throws Exception {
        if (series == null) {
            return;
        }
     /*   String freq; 
        switch (series.getFrequency().intValue()) {
            case 1:  freq = "Y";
                     break;
            case 3:  freq = "Q";
                     break;
            case 12:  freq = "M";
                     break;
            default: freq = "Not handled periodicity";
                     break;
        }*/
        
      //  series.getDomain().get(0).
        for (int i = 0; i < series.getLength(); ++i) {
             /*   writeTo.write(seriesName);
                writeTo.write(",");
                writeTo.write(seriesNameType);
                writeTo.write(",");
                writeTo.write(Integer.toString(series.getDomain().get(i).getYear()));
                writeTo.write(",");
                writeTo.write(freq);
                writeTo.write(",");
                writeTo.write(Integer.toString(series.getDomain().get(i).getPosition()+1));
                writeTo.write(",");
                double value = series.get(i);
                writeTo.write(Double.toString(Double.isNaN(value) ? -999999999 : value));
                writeTo.write("\r\n");*/
                
                double value = series.get(i);
                TEVList.add(Integer.toString(series.getDomain().get(i).getYear()));
                MHOList.add(Integer.toString(series.getDomain().get(i).getPosition()+1));
                NAMEList.add(seriesName);
                TYPEList.add(seriesNameType);
                VALUEList.add(value);
                
                
            }

        }
}
