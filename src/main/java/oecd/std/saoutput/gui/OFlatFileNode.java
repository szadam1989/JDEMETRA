/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package oecd.std.saoutput.gui;

import ec.nbdemetra.sa.output.AbstractOutputNode;
import ec.nbdemetra.sa.output.Series;
import ec.nbdemetra.ui.properties.NodePropertySetBuilder;
import ec.tss.sa.ISaOutputFactory;
import java.util.List;
import oecd.std.saoutput.OFlatFileConfiguration;
import oecd.std.saoutput.OFlatFileOutputFactory;
import org.openide.nodes.Sheet;

/**
 *
 * @author Jean Palate
 */
public class OFlatFileNode extends AbstractOutputNode<OFlatFileConfiguration> {

    public OFlatFileNode() {
        super(new OFlatFileConfiguration());
        setDisplayName(OFlatFileOutputFactory.NAME);
    }

    public OFlatFileNode(OFlatFileConfiguration config) {
        super(config);
        setDisplayName(OFlatFileOutputFactory.NAME);
    }

    @Override
    protected Sheet createSheet() {
        OFlatFileConfiguration config = getLookup().lookup(OFlatFileConfiguration.class);
        Sheet sheet = super.createSheet();
        NodePropertySetBuilder builder = new NodePropertySetBuilder();
        builder.reset("EMERALD");
      /*  builder.withFile().select(config, "Folder").directories(true).description("Base output folder. Will be extended by the workspace and processing names").add();
        sheet.put(builder.build());*/
        builder.with(String.class).select(config, "DatabaseTableName").display("Database Table Name").description("Name of the output database table.").add();
        sheet.put(builder.build());

        builder.reset("Content");
        builder.with(List.class).select(config, "Series").editor(Series.class).add();
        sheet.put(builder.build());
        
        builder.with(String.class).select(config, "YI36").display("YI36 - Becslés sorszáma").description("Egy 0 és 69 közé eső elemkód.").add();
        sheet.put(builder.build());
        
        builder.with(String.class).select(config, "YI37").display("YI37 - Szezonális kiigazítás javítás szerinti sorszáma").description("Egy 0 és 6 közé eső elemkód a becslés sorszámán belül.").add();
        sheet.put(builder.build());
        
       /* builder.reset("Layout");
        builder.withBoolean().select(config, "FullName").display("Full series name")
                .description("If true, the fully qualified name of the series will be used. "
                        + "If false, only the name of the series will be displayed.").add();
        sheet.put(builder.build());
        builder.with(String.class).select(config, "FullSeriesName").display("Fully qualified name of the series").description("Fully qualified name of the series - only reading data from database table. In case of Spreadsheets This field does not work.").add();
        sheet.put(builder.build());*/
        return sheet;
    }

    @Override
    public ISaOutputFactory getFactory() {
        return new OFlatFileOutputFactory(getLookup().lookup(OFlatFileConfiguration.class));
    }
}
