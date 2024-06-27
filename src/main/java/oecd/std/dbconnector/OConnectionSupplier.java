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

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.annotation.Nonnull;

/**
 * A class that supplies opened connections to databases. The parameters needed
 * to establish a connection are provided by a JdbcBean.
 *
 * @author Philippe Charles
 * @see http://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html
 */
public interface OConnectionSupplier {

    /**
     * Opens a connection to a database. The class that uses this connection
     * must close it after use.
     *
     * @return A new opened connection.
     * @throws SQLException
     */
    @Nonnull
    Connection getConnection(@Nonnull ODbBean bean) throws SQLException;

    /**
     * A connection supplier that uses {@link DriverManager}.
     */
    public static abstract class DriverBasedSupplier implements OConnectionSupplier {

        final boolean driverAvailable = loadDriver();

        @Override
        public Connection getConnection(ODbBean bean) throws SQLException {
            Preconditions.checkState(driverAvailable, "Driver not available");
            return DriverManager.getConnection(getUrl(bean));
        }

        public boolean isDriverAvailable() {
            return driverAvailable;
        }

        @Nonnull
        abstract protected String getUrl(@Nonnull ODbBean bean);

        abstract protected boolean loadDriver();
    }

    /**
     * A connection supplier that uses {@link javax.sql.DataSource}.
     */
    public static abstract class DataSourceBasedSupplier implements OConnectionSupplier {

        @Override
        public Connection getConnection(ODbBean bean) throws SQLException {
            return getDataSource(bean).getConnection();
        }

        @Nonnull
        abstract protected javax.sql.DataSource getDataSource(@Nonnull ODbBean bean) throws SQLException;
    }
}
