/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package oecd.std.saoutput.gui;

import ec.nbdemetra.sa.output.AbstractOutputNode;
import org.openide.util.lookup.ServiceProvider;
import ec.nbdemetra.sa.output.INbOutputFactory;
import oecd.std.saoutput.OFlatFileConfiguration;
import oecd.std.saoutput.OFlatFileOutputFactory;

/**
 *
 * @author Jean Palate
 */
@ServiceProvider(service = INbOutputFactory.class,
position = 2000)
public class OFlatFileFactory implements INbOutputFactory{
    
    private OFlatFileConfiguration config=new OFlatFileConfiguration();

    @Override
    public AbstractOutputNode createNode() {
        return new OFlatFileNode(config);
    }

    @Override
    public String getName() {
        return OFlatFileOutputFactory.NAME;
    }

    @Override
    public AbstractOutputNode createNodeFor(Object properties) {
        if (properties instanceof OFlatFileConfiguration)
            return new OFlatFileNode((OFlatFileConfiguration) properties);
        else
            return null;
    }

}
