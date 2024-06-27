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

import ec.tss.sa.output.BasicConfiguration;
import ec.tstoolkit.utilities.Jdk6;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

/**
 *
 * @author Kristof Bayens
 * @author Gyorgy Gyomai
 */
public class OFlatFileConfiguration extends BasicConfiguration implements Cloneable {
    public static final String[] defOutput = {"y", "t", "sa", "s", "i", "ycal"};// {"t", "sa"}
    private File folder_;
    private String databaseTableName_;
    private String YI36_;
    private String YI37_;
  //  private String fullSeriesName_;
    private String[] series_;
 //   private boolean fullName_;

    public OFlatFileConfiguration() {
        series_ = defOutput;
        databaseTableName_ = "";//Szilágyi Ádám
        YI36_ = "";//Szilágyi Ádám
        YI37_ = "";//Szilágyi Ádám
      /*  fullName_ = true;
        fullSeriesName_ = "actual";*///Szilágyi Ádám
    }

    public File getFolder() {
        return folder_;
    }

    public void setFolder(File value) {
        folder_ = value;
    }
    
    public String getDatabaseTableName() {//Szilágyi Ádám
        return databaseTableName_;
    }

    public void setDatabaseTableName(String value) {//Szilágyi Ádám
        databaseTableName_ = value;
    }
    
    public String getYI36() {//Szilágyi Ádám
        return YI36_;
    }

    public void setYI36(String value) {//Szilágyi Ádám
        YI36_ = value;
    }
    
    public String getYI37() {//Szilágyi Ádám
        return YI37_;
    }

    public void setYI37(String value) {//Szilágyi Ádám
        YI37_ = value;
    }
    
   /* public String getFullSeriesName() {//Szilágyi Ádám
        return fullSeriesName_;
    }

    public void setFullSeriesName(String value) {//Szilágyi Ádám
        fullSeriesName_ = value;
    }*/

    public List<String> getSeries() {
        return Arrays.asList(series_);
    }

    public void setSeries(List<String> value) {
        series_ = Jdk6.Collections.toArray(value, String.class);
    }

   /* public boolean isFullName() {
        return fullName_;
    }

    public void setFullName(boolean fullName) {
        fullName_ = fullName;
    }*/

    @Override
    public OFlatFileConfiguration clone() {
        try {
            return (OFlatFileConfiguration) super.clone();
        }
        catch (CloneNotSupportedException ex) {
            return null;
        }
    }
}
