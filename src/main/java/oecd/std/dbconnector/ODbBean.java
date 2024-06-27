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
package oecd.std.dbconnector;

import ec.tss.tsproviders.DataSource;
import static ec.tss.tsproviders.db.DbBean.X_AGGREGATION_TYPE;
import static ec.tss.tsproviders.db.DbBean.X_DATAFORMAT;
import static ec.tss.tsproviders.db.DbBean.X_DBNAME;
import static ec.tss.tsproviders.db.DbBean.X_DIMCOLUMNS;
import static ec.tss.tsproviders.db.DbBean.X_FREQUENCY;
import static ec.tss.tsproviders.db.DbBean.X_PERIODCOLUMN;
import static ec.tss.tsproviders.db.DbBean.X_TABLENAME;
import static ec.tss.tsproviders.db.DbBean.X_VALUECOLUMN;
import static ec.tss.tsproviders.db.DbBean.X_VERSIONCOLUMN;
import ec.tss.tsproviders.jdbc.JdbcBean;
import ec.tss.tsproviders.utils.IParam;
//import static ec.tss.tsproviders.utils.Params.onBoolean;
import static ec.tss.tsproviders.utils.Params.onString;
import javax.annotation.Nonnull;

/**
 *
 * @author Gyorgy Gyomai
 */
public class ODbBean extends JdbcBean {
 /*     public static final IParam<DataSource, Boolean> X_ISSTOREDPROCEDURE = onBoolean(false, "isStoredProcedure");
        public static final IParam<DataSource, String> X_STOREDPROCEDUREPARAMS = onString("", "storedProcedureParams");
        protected Boolean isStoredProcedure;
        protected String storedProcedureParams;*///4 sor kommentben volt
        public static final IParam<DataSource, String> X_FILTERPARAM = onString("", "filterParam");
        protected String filterParam;
     
    
    public ODbBean() {
        super();
     /*   this.isStoredProcedure = X_ISSTOREDPROCEDURE.defaultValue();
        this.storedProcedureParams = X_STOREDPROCEDUREPARAMS.defaultValue();*/
        this.filterParam = X_FILTERPARAM.defaultValue();
    }

    public ODbBean(@Nonnull DataSource id) {
        super(id);
      /*   this.isStoredProcedure = X_ISSTOREDPROCEDURE.get(id);
         this.storedProcedureParams = X_STOREDPROCEDUREPARAMS.get(id);*/
        this.filterParam = X_FILTERPARAM.get(id);
    }

        //<editor-fold defaultstate="collapsed" desc="Getters/Setters">

    
    public String getFilterParam() {
        return filterParam;
    }

    public void setFilterParam(String filterParam) {
        this.filterParam = filterParam;
    }
    
  /*  public boolean getIsStoredProcedure() {
        return isStoredProcedure;
    }

    public void setIsStoredProcedure(Boolean isStoredProcedure) {
        this.isStoredProcedure = isStoredProcedure;
    }

    public String getStoredProcedureParams() {
        return storedProcedureParams;
    }

    public void setStoredProcedureParams(String storedProcedureParams) {
        this.storedProcedureParams = storedProcedureParams;
    }*/
    

    //</editor-fold>
   
    @Override
    public DataSource toDataSource(String providerName, String version) {
        DataSource.Builder builder = DataSource.builder(providerName, version);
        X_DBNAME.set(builder, dbName);
        X_TABLENAME.set(builder, tableName);
        X_DIMCOLUMNS.set(builder, dimColumns);
        X_PERIODCOLUMN.set(builder, periodColumn);
        X_VALUECOLUMN.set(builder, valueColumn);
        X_DATAFORMAT.set(builder, dataFormat);
        X_VERSIONCOLUMN.set(builder, versionColumn);
        X_FILTERPARAM.set(builder, filterParam);
        X_FREQUENCY.set(builder, frequency);
        X_AGGREGATION_TYPE.set(builder, aggregationType);
     /*   X_ISSTOREDPROCEDURE.set(builder, isStoredProcedure);
        X_STOREDPROCEDUREPARAMS.set(builder, storedProcedureParams);*/

        X_CACHE_TTL.set(builder, cacheTtl);
        X_CACHE_DEPTH.set(builder, cacheDepth);
        return builder.build();
    }
}