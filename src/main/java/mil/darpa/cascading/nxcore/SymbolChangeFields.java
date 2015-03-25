package mil.darpa.cascading.nxcore;

import cascading.tuple.Fields;

/**
 *
 * @author Matt Parker
 */
public class SymbolChangeFields extends Fields {

    public SymbolChangeFields(){
        super("system_date", "system_time", "record_type", "symbol", "listed_exchange_old", "new_symbol", "listed_exchange");
    }
    
}
