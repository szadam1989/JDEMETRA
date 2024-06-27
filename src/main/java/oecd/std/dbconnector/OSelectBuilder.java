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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import ec.tstoolkit.design.IBuilder;
import ec.util.jdbc.SqlIdentifierQuoter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import static javax.swing.JOptionPane.showMessageDialog;
import org.netbeans.api.db.explorer.ConnectionManager;
import org.netbeans.api.db.explorer.DatabaseConnection;
import org.openide.util.Exceptions;

/**
 *
 * @author Philippe Charles
 */
public class OSelectBuilder implements IBuilder<String> {

    //
    private static final Joiner COMMA_JOINER = Joiner.on(',');
    private String table;
    private final List<String> select;
    private final List<String> filter;
    private final List<String> order;
    private boolean distinct;
    private SqlIdentifierQuoter identifierQuoter;
    private boolean isSP; 
    public String versionValue;

    public OSelectBuilder() {
        this.table = null;
        this.select = new ArrayList<>();
        this.filter = new ArrayList<>();
        this.order = new ArrayList<>();
        this.distinct = false;
        this.identifierQuoter = null;
        this.isSP = false;
   
    }

    @Nonnull
    private OSelectBuilder addIfNotNullOrEmpty(@Nonnull List<String> list, @Nonnull String... values) {
        for (String o : values) {
            if (!Strings.isNullOrEmpty(o)) {
                list.add(o);
            }
        }
        return this;
    }
    
    @Nonnull
    OSelectBuilder from(String table) {
        this.table = table;
        return this;
    }

    @Nonnull
    OSelectBuilder distinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }
    
    @Nonnull
    OSelectBuilder isSP(boolean isSP) {
        this.isSP = isSP;
        return this;
    }

    @Nonnull
    OSelectBuilder select(@Nonnull String... select) {
        return addIfNotNullOrEmpty(this.select, select);
    }

    @Nonnull
    OSelectBuilder filter(@Nonnull String... filter) {
        return addIfNotNullOrEmpty(this.filter, filter);
    }

    @Nonnull
    OSelectBuilder orderBy(@Nonnull String... order) {
        return addIfNotNullOrEmpty(this.order, order);
    }

    @Nonnull
    OSelectBuilder withQuoter(@Nonnull SqlIdentifierQuoter identifierQuoter) {
        this.identifierQuoter = identifierQuoter;
        return this;
    }

        
    @Override 
    public String build() {
        Function<String, String> toQuotedIdentifier = getIdentifierQuotingFunc();
        StringBuilder result = new StringBuilder();
        // SELECT
        result.append("SELECT ");
        if (distinct) {
            result.append("DISTINCT ");
        }
        COMMA_JOINER.appendTo(result, Iterables.transform(select, toQuotedIdentifier));
        // FROM
        result.append(" FROM ").append(toQuotedIdentifier.apply(table));
        // WHERE
        if (!filter.isEmpty()) {
            result.append(" WHERE ");
            Iterator<String> iter = Iterables.transform(filter, toQuotedIdentifier).iterator();
            result.append(iter.next());//.append("=?"); //Szilágyi Ádám
            while (iter.hasNext()) {
                result.append(" AND ").append(iter.next()).append("=?");
            }
        }
        // ORDER BY
        if (!order.isEmpty()) {
            result.append(" ORDER BY ");
            COMMA_JOINER.appendTo(result, Iterables.transform(order, toQuotedIdentifier));
        }
     //   showMessageDialog(null, result.toString());
     //   showMessageDialog(null, System.getProperty("user.name"));
     
     
        
        return result.toString();
    }

    private Function<String, String> getIdentifierQuotingFunc() {
        return identifierQuoter != null ? new IdentifierQuotingFunc(identifierQuoter) : Functions.<String>identity();
    }

    private static final class IdentifierQuotingFunc implements Function<String, String> {

        private final SqlIdentifierQuoter identifierQuoter;

        public IdentifierQuotingFunc(@Nonnull SqlIdentifierQuoter identifierQuoter) {
            this.identifierQuoter = identifierQuoter;
        }

        @Override
        public String apply(String identifier) {
            return identifierQuoter.quote(identifier, false);
        }
    }
}
