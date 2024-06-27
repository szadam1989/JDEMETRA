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

import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.db.DbSeries;
import ec.tss.tsproviders.db.DbSetId;
import ec.tss.tsproviders.db.DbUtil;
import ec.tss.tsproviders.jdbc.ResultSetFunc;

import ec.util.jdbc.SqlIdentifierQuoter;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import static javax.swing.JOptionPane.showMessageDialog;
import static oecd.std.dbconnector.OJdbcAccessor.CALLTYPE.*;
import org.netbeans.api.db.explorer.ConnectionManager;
import org.netbeans.api.db.explorer.DatabaseConnection;
import org.openide.util.Exceptions;
import org.slf4j.Logger;
import com.google.common.base.Optional;

/**
 *
 * @author Demortier Jeremy
 * @author Philippe Charles
 * @author Gyorgy Gyomai
 */
public class OJdbcAccessor<BEAN extends ODbBean> extends DbAccessor.Commander<BEAN> {

    protected final Logger logger;
    protected final OConnectionSupplier supplier;
    public enum CALLTYPE {Children, AllSeries, AllSeriesWithData, SeriesWithData, ColumnValidation, ElementValidation};  

    public OJdbcAccessor(@Nonnull Logger logger, @Nonnull BEAN dbBean, @Nonnull OConnectionSupplier supplier) {
        super(dbBean);
        this.logger = logger;
        this.supplier = supplier;
    }

    @Override
    public Exception testDbBean() {
       // test if database/tablename/value and period field as filled in
        Exception result = super.testDbBean();
        if (result != null) {
            return result;
        }
        try (Connection conn = supplier.getConnection(dbBean)) {
            DatabaseMetaData metaData = conn.getMetaData();
            String tableName = dbBean.getTableName(); //it is used for stored procedures as well
            Boolean isTableFound = false; //it is used for stored procedures as well
            
            com.google.common.base.Optional<DatabaseConnection> optDBConn = com.google.common.base.Optional.absent();
        for (DatabaseConnection c : ConnectionManager.getDefault().getConnections()) {
            optDBConn = com.google.common.base.Optional.of(c);
        }
    
            /*    if (dbBean.getIsStoredProcedure()){

                    ResultSet lProcedures = metaData.getProcedures(null, null, null);

                    while(lProcedures.next()) {
                        if (tableName.equals(lProcedures.getString(3))) {
                        isTableFound = true;
                        }
                    }
                    
                    if (!isTableFound){
                    return new Exception("Table named '" + tableName + "' does not exist");
                    }
                }
                else {*/
                    
                    ResultSet lTables = metaData.getTables(null, null, null, null);

                    while(lTables.next()) {
                        if (tableName.equals(lTables.getString(3))) {
                        isTableFound = true;
                        }
                    }
                    
                    if (!isTableFound){
                    return new Exception("Table named '" + tableName + "' does not exist");
                    }
             //   }
            return null;
        } catch (SQLException ex) {
            return ex;
        }
    }

    /**
     * Creates a function that returns a child id from the current record of a
     * ResultSet.
     *
     * @param metaData
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    @Nonnull
    protected ResultSetFunc<String> getChildFunc(@Nonnull ResultSetMetaData metaData, int columnIndex) throws SQLException {
        return ResultSetFunc.onGetString(columnIndex);
    }

    /**
     * Creates a function that returns dimension values from the current record
     * of a ResultSet.
     *
     * @param metaData
     * @param firstColumnIndex
     * @param length
     * @return
     * @throws SQLException
     */
    @Nonnull
    protected ResultSetFunc<String[]> getDimValuesFunc(@Nonnull ResultSetMetaData metaData, int firstColumnIndex, int length) throws SQLException {
        return ResultSetFunc.onGetStringArray(firstColumnIndex, length);
    }

    /**
     * Creates a function that returns a period from the current record of a
     * ResultSet.
     *
     * @param metaData
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    @Nonnull
    protected ResultSetFunc<java.util.Date> getPeriodFunc(@Nonnull ResultSetMetaData metaData, int columnIndex) throws SQLException {
        return ResultSetFunc.onDate(metaData, columnIndex, dateParser);
    }

    /**
     * Creates a function that returns a value from the current record of a
     * ResultSet.
     *
     * @param metaData
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    @Nonnull
    protected ResultSetFunc<Number> getValueFunc(@Nonnull ResultSetMetaData metaData, int columnIndex) throws SQLException {
        return ResultSetFunc.onNumber(metaData, columnIndex, numberParser);
    }

    @Override
    protected Callable<List<DbSetId>> getAllSeriesQuery(DbSetId ref) {
        logger.info("all series query");
        return new JdbcQuery<List<DbSetId>>(ref, AllSeries) {
            @Override
            protected String getQueryString(DatabaseMetaData metaData) throws SQLException {
                OSelectBuilder o = new OSelectBuilder(); 
                return o.from(getDbBean().getTableName())
                        .distinct(true)
                        .select(ref.selectColumns())
                        .filter(dbBean.getVersionColumn() + " = " + dbBean.filterParam)//ref.filterColumns()
                        .orderBy(ref.selectColumns())
                        .withQuoter(SqlIdentifierQuoter.create(metaData))
                        .build();
            }

            @Override
            protected List<DbSetId> process(final ResultSet rs) throws SQLException {
                final ResultSetFunc<String[]> toDimValues = getDimValuesFunc(rs.getMetaData(), 1, ref.getDepth());

                DbUtil.AllSeriesCursor<SQLException> cursor = new DbUtil.AllSeriesCursor<SQLException>() {
                    @Override
                    public boolean next() throws SQLException {
                        boolean result = rs.next();
                        if (result) {
                            dimValues = toDimValues.apply(rs);
                        }
                        return result;
                    }
                };

                return DbUtil.getAllSeries(cursor, ref);
            }
        };
    }

    @Override 
    protected Callable<List<DbSeries>> getAllSeriesWithDataQuery(DbSetId ref) {
        logger.info("all series with data query");
        return new JdbcQuery<List<DbSeries>>(ref, AllSeriesWithData) {
            @Override
            protected String getQueryString(DatabaseMetaData metaData) throws SQLException {
                ODbBean dbBean = getDbBean();
                
                OSelectBuilder o = new OSelectBuilder(); 
             //   showMessageDialog(null, "1.");
                return o.from(getDbBean().getTableName())
                        .select(ref.selectColumns()).select(dbBean.getPeriodColumn(), dbBean.getValueColumn())
                        .filter(dbBean.getVersionColumn() + " = " + dbBean.filterParam)//ref.filterColumns()
                        .orderBy(ref.selectColumns()).orderBy(dbBean.getPeriodColumn(), dbBean.getVersionColumn())
                        .withQuoter(SqlIdentifierQuoter.create(metaData))
                        .build();
            }

            @Override
            protected List<DbSeries> process(final ResultSet rs) throws SQLException {
                // Beware that some jdbc drivers require to get the columns values 
                // in the order of the query and only once.
                // So, call the following methods once per row and in this order.
                ResultSetMetaData metaData = rs.getMetaData();
                final ResultSetFunc<String[]> toDimValues = getDimValuesFunc(metaData, 1, ref.getDepth());
                final ResultSetFunc<java.util.Date> toPeriod = getPeriodFunc(metaData, ref.getDepth() + 1);
                final ResultSetFunc<Number> toValue = getValueFunc(metaData, ref.getDepth() + 2);

                DbUtil.AllSeriesWithDataCursor<SQLException> cursor = new DbUtil.AllSeriesWithDataCursor<SQLException>() {
                    @Override
                    public boolean next() throws SQLException {
                        boolean result = rs.next();
                        if (result) {
                            dimValues = toDimValues.apply(rs);
                            period = toPeriod.apply(rs);
                            value = period != null ? toValue.apply(rs) : null;
                        }
                        return result;
                    }
                };

                ODbBean dbBean = getDbBean();
                return DbUtil.getAllSeriesWithData(cursor, ref, dbBean.getFrequency(), dbBean.getAggregationType());
            }
        };
    }

    @Override
    protected Callable<DbSeries> getSeriesWithDataQuery(DbSetId ref) {
        logger.info("single series with data query");
        return new JdbcQuery<DbSeries>(ref, SeriesWithData) {
            @Override
            protected String getQueryString(DatabaseMetaData metaData) throws SQLException {
                ODbBean dbBean = getDbBean();
                OSelectBuilder o = new OSelectBuilder(); 
               // showMessageDialog(null, "2.");
                return o.from(getDbBean().getTableName())
                        .select(dbBean.getPeriodColumn(), dbBean.getValueColumn())
                        .filter(dbBean.getVersionColumn() + " = " + dbBean.filterParam)//ref.filterColumns()
                        .orderBy(dbBean.getPeriodColumn(), dbBean.getVersionColumn())
                        .withQuoter(SqlIdentifierQuoter.create(metaData))
                        .build();
            }

            @Override
            protected DbSeries process(final ResultSet rs) throws SQLException {
                // Beware that some jdbc drivers require to get the columns values 
                // in the order of the query and only once.
                // So, call the following methods once per row and in this order.
                ResultSetMetaData metaData = rs.getMetaData();
                final ResultSetFunc<java.util.Date> toPeriod = getPeriodFunc(metaData, 1);
                final ResultSetFunc<Number> toValue = getValueFunc(metaData, 2);

                DbUtil.SeriesWithDataCursor<SQLException> cursor = new DbUtil.SeriesWithDataCursor<SQLException>() {
                    @Override
                    public boolean next() throws SQLException {
                        boolean result = rs.next();
                        if (result) {
                            period = toPeriod.apply(rs);
                            value = period != null ? toValue.apply(rs) : null;
                        }
                        return result;
                    }
                };

                ODbBean dbBean = getDbBean();
                return DbUtil.getSeriesWithData(cursor, ref, dbBean.getFrequency(), dbBean.getAggregationType());
            }
        };
    }

    @Override
    protected Callable<List<String>> getChildrenQuery(DbSetId ref) {
        logger.info("browsing context query");
        return new JdbcQuery<List<String>>(ref, Children) {
            @Override
            protected String getQueryString(DatabaseMetaData metaData) throws SQLException {
                
        Optional<DatabaseConnection> optDBConn = Optional.absent();
        for (DatabaseConnection c : ConnectionManager.getDefault().getConnections()) {
            optDBConn = Optional.of(c);
        }
                String sql = null;
        
        try{
            DatabaseConnection dataconn = optDBConn.get();
            Connection conn = dataconn.getJDBCConnection();
            
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
            LocalDateTime now = LocalDateTime.now();  
            
            Statement statement = conn.createStatement();

                sql = "INSERT INTO YI_J.INPUT_LOG(USER_ID, DataSourceName, TableName, DimensionColumns, PeriodColumn, ValueColumn, VersionColumn, ValueofVersionColumn, SelectDateTime) VALUES('" + 
                        System.getProperty("user.name") + "', '" + dbBean.getDbName() + "', '" + dbBean.getTableName() + "', '" + 
                        dbBean.getDimColumns() + "', '" + dbBean.getPeriodColumn() + "', '" + dbBean.getValueColumn()+ "', '" + 
                        dbBean.getVersionColumn()+ "', '" + dbBean.getFilterParam() + "', TO_DATE('" + dtf.format(now) + "', 'YYYY/MM/DD HH24:MI:SS'))";

                statement.executeUpdate(sql);
          
           
        } catch (SQLException ex) {
            showMessageDialog(null, "A program futtatása során hibajelzés következett be, amely a jobb alsó sarokban olvasható. A művelet végrehajtása sikertelen!");
            showMessageDialog(null, sql);
            Exceptions.printStackTrace(ex);
        }
        
                String column = ref.getColumn(ref.getLevel());
                OSelectBuilder o = new OSelectBuilder(); 
                return o.from(getDbBean().getTableName())
                        .distinct(true)
                        .select(column)
                        .filter(dbBean.getVersionColumn() + " = " + dbBean.filterParam)//ref.filterColumns()
                        .orderBy(column)
                        .withQuoter(SqlIdentifierQuoter.create(metaData))
                        .build();
            }

            @Override
            protected List<String> process(final ResultSet rs) throws SQLException {
                final ResultSetFunc<String> toChild = getChildFunc(rs.getMetaData(), 1);

                DbUtil.ChildrenCursor<SQLException> cursor = new DbUtil.ChildrenCursor<SQLException>() {
                    @Override
                    public boolean next() throws SQLException {
                        boolean result = rs.next();
                        if (result) {
                            child = toChild.apply(rs);
                        }
                        return result;
                    }
                };

                return DbUtil.getChildren(cursor);
            }
        };
    }

    @Override
    public DbAccessor<BEAN> memoize() {
        return DbAccessor.BulkAccessor.from(this, dbBean.getCacheDepth(), DbAccessor.BulkAccessor.newTtlCache(dbBean.getCacheTtl()));
    }

    /**
     * An implementation of Callable that handles SQL queries from Jdbc.
     *
     * @param <T>
     */
    protected abstract class JdbcQuery<T> implements Callable<T> {

        protected final DbSetId ref;
        protected final CALLTYPE callType;

        protected JdbcQuery(@Nonnull DbSetId ref, @Nonnull CALLTYPE callType) {
            this.ref = ref;
            this.callType = callType;
        }

        /**
         * Creates an SQL statement that may contain one or more '?' IN
         * parameter placeholders
         *
         * @return a SQL statement
         */
        @Nonnull
        abstract protected String getQueryString(DatabaseMetaData metaData) throws SQLException;           

        protected void setParameters(@Nonnull PreparedStatement statement) throws SQLException {
            for (int i = 0; i < ref.getLevel(); i++) {
                statement.setString(i + 1, ref.getValue(i));
                logger.info((i+1) +": " + ref.getValue(i) );
            }
        }

        /**
         * Process the specified ResultSet in order to create the expected
         * result.
         *
         * @param rs the ResultSet to be processed
         * @return
         * @throws SQLException
         */
        @Nullable
        abstract protected T process(@Nonnull ResultSet rs) throws SQLException;

        @Override
        public T call() throws SQLException {
            ODbBean dbBean = getDbBean();
            synchronized (dbBean) {
                try (Connection conn = supplier.getConnection(dbBean)) {
                 /*   if (dbBean.getIsStoredProcedure()){
                        PreparedStatement cmd = new OStatementBuilder(callType, dbBean, ref, conn, logger).build();
                        try (ResultSet rs = cmd.executeQuery()) {
                                return process(rs);
                        }
                    }
                    else{*/
                        String queryString = getQueryString(conn.getMetaData());
                        logger.info(queryString);
                        try (PreparedStatement cmd = conn.prepareStatement(queryString)) {
                            setParameters(cmd);
                            try (ResultSet rs = cmd.executeQuery()) {
                                return process(rs);
                        }
                    }
                 //   }
                }
            }
        }
    }
}
