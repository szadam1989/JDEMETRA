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
package oecd.std.dbconnector.gui;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import ec.nbdemetra.db.DbIcon;
import ec.nbdemetra.db.DbProviderBuddy;
import ec.nbdemetra.ui.Config;
import ec.nbdemetra.ui.IConfigurable;
import ec.nbdemetra.ui.awt.SimpleHtmlListCellRenderer;
import ec.nbdemetra.ui.properties.NodePropertySetBuilder;
import ec.nbdemetra.ui.tsproviders.IDataSourceProviderBuddy;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.TsProviders;
import ec.tss.tsproviders.db.DbBean;
import ec.tss.tsproviders.utils.DataFormat;
import ec.tstoolkit.timeseries.TsAggregationType;
import ec.tstoolkit.timeseries.simplets.TsFrequency;

import static ec.util.chart.impl.TangoColorScheme.DARK_ORANGE;
import static ec.util.chart.impl.TangoColorScheme.DARK_SCARLET_RED;
import static ec.util.chart.swing.SwingColorSchemeSupport.rgbToColor;
import ec.util.completion.AbstractAutoCompletionSource.TermMatcher;
import ec.util.completion.AutoCompletionSource;
import ec.util.completion.AutoCompletionSource.Behavior;
import ec.util.completion.ext.QuickAutoCompletionSource;
import ec.util.jdbc.ForwardingConnection;
import ec.util.various.swing.FontAwesome;
import java.awt.EventQueue;
import java.awt.Image;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.swing.Action;
import javax.swing.ListCellRenderer;
import javax.swing.SwingUtilities;
import oecd.std.dbconnector.OConnectionSupplier;
import oecd.std.dbconnector.ODbBean;
import oecd.std.dbconnector.OJndiJdbcProvider;
import org.netbeans.api.db.explorer.ConnectionManager;
import org.netbeans.api.db.explorer.DatabaseConnection;
import org.netbeans.api.db.explorer.DatabaseException;
import org.openide.filesystems.FileUtil;
import org.openide.nodes.PropertySupport;
import org.openide.nodes.Sheet;
import org.openide.util.ImageUtilities;
import org.openide.util.NbBundle;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 * @author Gyorgy Gyomai (modified)
 */
@ServiceProvider(service = IDataSourceProviderBuddy.class)
public class OJndiJdbcProviderBuddy extends DbProviderBuddy<ODbBean> implements IConfigurable {

    private final static Config EMPTY = Config.builder("", "", "").build();
    private final AutoCompletionSource dbSource;
    protected final OConnectionSupplier supplier;
    
    private final ListCellRenderer dbRenderer;
    private final Image warningBadge;
    private final Image errorBadge;

    public OJndiJdbcProviderBuddy() {
        this.supplier= new DbExplorerConnectionSupplier();
        this.dbSource = new DbExplorerSource();
        this.dbRenderer = new DbExplorerRenderer();
        
        this.warningBadge = FontAwesome.FA_EXCLAMATION_TRIANGLE.getImage(rgbToColor(DARK_ORANGE), 8f);
        this.errorBadge = FontAwesome.FA_EXCLAMATION_CIRCLE.getImage(rgbToColor(DARK_SCARLET_RED), 8f);
        // this overrides default connection supplier since we don't have JNDI in JavaSE
        TsProviders.lookup(OJndiJdbcProvider.class, OJndiJdbcProvider.SOURCE).get().setConnectionSupplier(supplier);
    }

    @Override
    public String getProviderName() {
        return OJndiJdbcProvider.SOURCE;
    }

    @Override
    public Image getIcon(int type, boolean opened) {
        return DbIcon.DATABASE.getImageIcon().getImage();
    }

    @Override
    public Image getIcon(DataSource dataSource, int type, boolean opened) {
        Image image = super.getIcon(dataSource, type, opened);
        String dbName = DbBean.X_DBNAME.get(dataSource);
        switch (DbConnStatus.lookupByDisplayName(dbName)) {
            case DISCONNECTED:
                return ImageUtilities.mergeImages(image, warningBadge, 8, 8);
            case MISSING:
                return ImageUtilities.mergeImages(image, errorBadge, 8, 8);
        }
        return image;
    }

    @Override
    protected List<Sheet.Set> createSheetSets(DataSource dataSource) {
        List<Sheet.Set> result = super.createSheetSets(dataSource);
        NodePropertySetBuilder b = new NodePropertySetBuilder().name("Connection");
        b.add(new DbConnStatusProperty(DbBean.X_DBNAME.get(dataSource)));
        result.add(b.build());
        return result;
    }

    @Override
    protected boolean isFile() {
        return false;
    }

    @Override
    protected AutoCompletionSource getDbSource(ODbBean bean) {
        return dbSource;
    }

    @Override
    protected ListCellRenderer getDbRenderer(ODbBean bean) {
        return dbRenderer;
    }
    
     @NbBundle.Messages({
        "bean.dbName.display=Data source name",
        "bean.dbName.description=Data structure describing the connection to the database.",
        "bean.tableName.display=Table name",
        "bean.tableName.description=The name of the table (or view) that contains observations.",
        "bean.dimColumns.display=Dimension column",
        "bean.dimColumns.description=A column name that defines the dimension of the table.",
        "bean.periodColumn.display=Period column",
        "bean.periodColumn.description=A column name that defines the period of an observation.",
        "bean.valueColumn.display=Value column",
        "bean.valueColumn.description=A column name that defines the value of an observation.",
        "bean.versionColumn.display=Version column",
        "bean.versionColumn.description=A column name that defines the version of an observation.",
        "bean.filterParam.display=Value of Version column",
        "bean.filterParam.description=Szűrés a verzióoszlop értéke alapján. Nem maradhat üresen!",
        "bean.dataFormat.display=Data format",
        "bean.dataFormat.description=The format used to parse dates and numbers from character strings.",
        "bean.frequency.display=Frequency",
        "bean.frequency.description=The frequency of the observations in the table. An undefined frequency allows the provider to guess it.",
        "bean.aggregationType.display=Aggregation type",
        "bean.aggregationType.description=The aggregation method to use when a frequency is defined."
      /*  "bean.isStoredProcedure.display=Is it a stored procedure?",
        "bean.isStoredProcedure.description=Changes the behaviour of the TableName field. It expects a stored procedure with the right dimensional structure.",
        "bean.storedProcedureParams.display=Stored procedure parameters",
        "bean.storedProcedureParams.description=Parameters to set the basic domain context."*/})
    @Nonnull
    @Override
    protected NodePropertySetBuilder withOptions(@Nonnull NodePropertySetBuilder b, @Nonnull ODbBean bean) {
        b.with(DataFormat.class)
                .select(bean, "dataFormat")
                .display(Bundle.bean_dataFormat_display())
                .description(Bundle.bean_dataFormat_description())
                .add();
        b.withEnum(TsFrequency.class)
                .select(bean, "frequency")
                .display(Bundle.bean_frequency_display())
                .description(Bundle.bean_frequency_description())
                .add();
        b.withEnum(TsAggregationType.class)
                .select(bean, "aggregationType")
                .display(Bundle.bean_aggregationType_display())
                .description(Bundle.bean_aggregationType_description())
                .add();
       /*  b.with(String.class)
                .select(bean, "filterParam")
                .display(Bundle.bean_filterParam_display())
                .description(Bundle.bean_filterParam_description())
                .add();
       b.with(Boolean.class)
                .select(bean, "isStoredProcedure")
                .display(Bundle.bean_isStoredProcedure_display())
                .description(Bundle.bean_isStoredProcedure_description())
                .add();
        b.with(String.class)
                .select(bean, "storedProcedureParams")
                .display(Bundle.bean_storedProcedureParams_display())
                .description(Bundle.bean_storedProcedureParams_description())
                .add();*/
        return b;
    }
    
    
    @Override
    protected NodePropertySetBuilder withSource(@Nonnull NodePropertySetBuilder b, @Nonnull ODbBean bean) {
        b.with(String.class)
                .select(bean, "dbName")
                .display(Bundle.bean_dbName_display())
                .description(Bundle.bean_dbName_description())
                .add();
        b.with(String.class)
                .select(bean, "tableName")
                .display(Bundle.bean_tableName_display())
                .description(Bundle.bean_tableName_description())
                .add();
        b.with(String.class)
                .select(bean, "dimColumns")
                .display(Bundle.bean_dimColumns_display())
                .description(Bundle.bean_dimColumns_description())
                .add();
        b.with(String.class)
                .select(bean, "periodColumn")
                .display(Bundle.bean_periodColumn_display())
                .description(Bundle.bean_periodColumn_description())
                .add();
        b.with(String.class)
                .select(bean, "valueColumn")
                .display(Bundle.bean_valueColumn_display())
                .description(Bundle.bean_valueColumn_description())
                .add();
        b.with(String.class)
                .select(bean, "versionColumn")
                .display(Bundle.bean_versionColumn_display())
                .description(Bundle.bean_versionColumn_description())
                .add();
        b.with(String.class)
                .select(bean, "filterParam")
                .display(Bundle.bean_filterParam_display())
                .description(Bundle.bean_filterParam_description())
                .add();
        return b;
    }

    //<editor-fold defaultstate="collapsed" desc="Config methods">
    @Override
    public Config getConfig() {
        return EMPTY;
    }

    @Override
    public void setConfig(Config config) throws IllegalArgumentException {
        Preconditions.checkArgument(config.equals(EMPTY));
    }

    @Override
    public Config editConfig(Config config) throws IllegalArgumentException {
        final Action openServicesTab = FileUtil.getConfigObject("Actions/Window/org-netbeans-core-ide-ServicesTabAction.instance", Action.class);
        if (openServicesTab != null) {
            EventQueue.invokeLater(new Runnable() {
                @Override
                public void run() {
                    openServicesTab.actionPerformed(null);
                }
            });
        }
        return EMPTY;
    }
    //</editor-fold>

    private enum DbConnStatus {

        CONNECTED,
        DISCONNECTED,
        MISSING;

        public static DbConnStatus lookupByDisplayName(String dbName) {
            
            for (DatabaseConnection c : ConnectionManager.getDefault().getConnections()) {
            if (c.getDisplayName().equals(dbName)) {   
                if (c.getJDBCConnection() != null) {
                    return  CONNECTED;
                }
                else {
                    return DISCONNECTED;
                }
            }
            }
            return MISSING;        
        }
    }

    private static final class DbExplorerSource extends QuickAutoCompletionSource<DatabaseConnection> {

        @Override
        public Behavior getBehavior(String term) {
            return Behavior.NONE;
        }

        @Override
        protected String getValueAsString(DatabaseConnection value) {
            return value.getDisplayName();
        }

        @Override
        protected Iterable<DatabaseConnection> getAllValues() throws Exception {
            return Arrays.asList(ConnectionManager.getDefault().getConnections());
        }

        @Override
        protected boolean matches(TermMatcher termMatcher, DatabaseConnection input) {
            return termMatcher.matches(input.getName()) || termMatcher.matches(input.getDisplayName());
        }
    }

    private static final class DbExplorerRenderer extends SimpleHtmlListCellRenderer<DatabaseConnection> {

        public DbExplorerRenderer() {
            super(new SimpleHtmlListCellRenderer.HtmlProvider<DatabaseConnection>() {
                @Override
                public String getHtmlDisplayName(DatabaseConnection value) {
                    return "<html><b>" + value.getDisplayName() + "</b> - <i>" + value.getName() + "</i>";
                }
            });
        }
    }
    
    //private static final
    //Szilágyi Ádám private to public
    public static class DbExplorerConnectionSupplier implements OConnectionSupplier {

        @Override
        public Connection getConnection(ODbBean bean) throws SQLException {
            return getJDBCConnection(bean.getDbName());
        }
        //private
        public Connection getJDBCConnection(String dbName) throws SQLException {
            Optional<DatabaseConnection> o = Optional.absent();
            for (DatabaseConnection c : ConnectionManager.getDefault().getConnections()) {
            if (c.getDisplayName().equals(dbName)) {
                o = Optional.of(c);
            }
            }
           
            if (o.isPresent()) {
                return getJDBCConnection(o.get());
            }
            throw new SQLException("Cannot find connection named '" + dbName + "'");
        }

        //private 
        //Szilágyi Ádám private to public
        public Connection getJDBCConnection(DatabaseConnection o) throws SQLException {
            Connection conn = o.getJDBCConnection();
            if (conn != null) {
                return new FailSafeConnection(conn, o.getSchema());
            }
            if (openJDBCConnection(o)) {
                return new FailSafeConnection(o.getJDBCConnection(), o.getSchema());
            }
            throw new SQLException("Not connected to the database");
        }
        //private
        public boolean openJDBCConnection(DatabaseConnection o) throws SQLException {
            // let's try to connect without opening any dialog
            // must be done outside EDT
            if (!SwingUtilities.isEventDispatchThread()) {
                try {
                    // currently, the manager returns false if user is empty
                    // this behavior prevents some connections but it might be fixed in further versions
                    return ConnectionManager.getDefault().connect(o);
                } catch (DatabaseException ex) {
                    throw new SQLException("Failed to connect to the database", ex);
                }
            }
            return false;
        }
    }

    private static final class FailSafeConnection extends ForwardingConnection {

        private final Connection delegate;
        private final String defaultSchema;

        public FailSafeConnection(@Nonnull Connection delegate, String defaultSchema) {
            this.delegate = delegate;
            this.defaultSchema = defaultSchema;
        }

        @Override
        protected Connection getConnection() {
            return delegate;
        }

        @Override
        public String getSchema() throws SQLException {
            try {
                String result = super.getSchema();
                return result != null ? result : defaultSchema;
            } catch (SQLException | AbstractMethodError ex) {
                // occurs when :
                // - method is not yet implemented
                // - driver follows specs older than JDBC4 specs
                return defaultSchema;
            }
        }

        @Override
        public void close() throws SQLException {
            // Calling Connection#close() is forbidden
            // See DatabaseConnection#getJDBCConnection()
        }
    }

    private static final class DbConnStatusProperty extends PropertySupport.ReadOnly<String> {

        private final String dbName;

        public DbConnStatusProperty(String dbName) {
            super("Status", String.class, null, null);
            this.dbName = dbName;
        }

        @Override
        public String getValue() throws IllegalAccessException, InvocationTargetException {
            return DbConnStatus.lookupByDisplayName(dbName).name();
        }
    }
}
