/*
 * Copyright 2013 National Bank of Belgium
 * 
 * Licensed under the EUPL, Version 1.1 or - as soon they will be approved 
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

import ec.tss.ITsProvider;
import ec.tss.TsAsyncMode;
import ec.tss.TsMoniker;
import ec.tss.tsproviders.DataSet;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.db.DbProvider;

import ec.tss.tsproviders.utils.Parsers;

import java.sql.SQLException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import oecd.std.dbconnector.OConnectionSupplier.DataSourceBasedSupplier;
import org.openide.util.lookup.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic Jdbc provider that uses Jndi as a connection supplier. <p>Note that
 * you can supply you own connection supplier by using
 * {@link #setConnectionSupplier(ec.tss.tsproviders.jdbc.ConnectionSupplier)}
 * method. It is useful when running under JavaSE since Jndi is not available by
 * default in this environment.
 *
 * @author Philippe Charles
 * @see http://docs.oracle.com/javase/tutorial/jdbc/basics/sqldatasources.html
 * @see javax.sql.DataSource
 * @see Context#lookup(java.lang.String)
 */
@ServiceProvider(service = ITsProvider.class)
public class OJndiJdbcProvider extends DbProvider<ODbBean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OJndiJdbcProvider.class);
    public static final String SOURCE = "OJNDI-JDBC";
    private static final String VERSION = "20160201";
    // PROPERTIES
    private static final DataSourceBasedSupplier DEFAULT_SUPPLIER = new JndiJdbcSupplier();
    private OConnectionSupplier connectionSupplier;

    //TODO Analyse these to see what is their role
    protected final Parsers.Parser<DataSource> legacyDataSourceParser;
    protected final Parsers.Parser<DataSet> legacyDataSetParser;
    
    public OJndiJdbcProvider() {
        super(LOGGER, SOURCE ,TsAsyncMode.Once); 
        this.connectionSupplier = DEFAULT_SUPPLIER;
        legacyDataSourceParser= null;
        legacyDataSetParser=null;
    }

    @Override
    protected DbAccessor<ODbBean> loadFromBean(ODbBean bean) throws Exception {
        return new OJdbcAccessor(logger, bean, connectionSupplier).memoize();
    }

    @Override
    public ODbBean newBean() {
        return new ODbBean();
    }

    @Override
    public ODbBean decodeBean(DataSource dataSource) throws IllegalArgumentException {
        return new ODbBean(dataSource);
    }
    
    @Override
    public DataSet toDataSet(TsMoniker moniker) {
        DataSet result = super.toDataSet(moniker);
        return result != null ? result : legacyDataSetParser.parse(moniker.getId());
    }

    @Override
    public DataSource toDataSource(TsMoniker moniker) {
        DataSource result = super.toDataSource(moniker);
        return result != null ? result : legacyDataSourceParser.parse(moniker.getId());
    }
    
    
     @Override
    public DataSource encodeBean(Object bean) throws IllegalArgumentException {
        return support.checkBean(bean, ODbBean.class).toDataSource(getSource(), VERSION);
    }

    @Override
    public String getDisplayName() {
        return "HCSO JDBC Insert into Database";
    }
    
    @Override
    public String getDisplayName(DataSet dataSet) {
       return super.getDisplayNodeName(dataSet);
    }
    
    @Nonnull
    public OConnectionSupplier getConnectionSupplier() {
        return connectionSupplier;
    }

    public void setConnectionSupplier(@Nullable OConnectionSupplier connectionSupplier) {
        this.connectionSupplier = connectionSupplier != null ? connectionSupplier : DEFAULT_SUPPLIER;
    }

    @Override
    public void close() {
        super.close(); //To change body of generated methods, choose Tools | Templates.
    }

    private final static class JndiJdbcSupplier extends DataSourceBasedSupplier {

        @Override
        protected javax.sql.DataSource getDataSource(ODbBean bean) throws SQLException {
            try {
                Context ctx = new InitialContext();
                return (javax.sql.DataSource) ctx.lookup(bean.getDbName());
            } catch (NamingException ex) {
                throw new SQLException("Cannot retrieve javax.sql.DataSource object", ex);
            }
        }
    }
}
