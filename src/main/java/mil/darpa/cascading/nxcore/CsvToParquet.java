package mil.darpa.cascading.nxcore;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import java.util.Properties;
import parquet.cascading.ParquetTupleScheme;

/**
 * Migrate compressed CSV file to Parquet file format.
 *
 * @author Matt Parker
 */
public class CsvToParquet {

    private static final String SYMBOL_CHANGE = "symbol_change";
    private static final String EXG_QUOTE = "exg_quote";
    private static final String TRADE = "trade";
    private static final String MMQUOTE_FILE = "mmquote_file";
    
    private static final boolean NO_HEADER = false;
    
    private static final String CSV_FIELD_DELIMITER = ",";
    private static final String CONVERT_PIPELINE = "convert";
    private static final String PARQUET_DATA_FLOW = "parquet-data-flow";
    private static final String CSV_TO_PARQUET_ETL = "csv-to-parquet-etl";
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        String hdfsInputPath = args[0];
        String hdfsOutputPath = args[1];
        String fieldDefinition = args[2];
        
        Properties properties = AppProps.appProps()
        .setName( CSV_TO_PARQUET_ETL )
        .setJarClass( CsvToParquet.class )
        .buildProperties();

        FlowConnector connector = new HadoopFlowConnector( properties );
        
        Fields fields = null;
        
        if ( fieldDefinition.equalsIgnoreCase( SYMBOL_CHANGE ) ){
            fields = new SymbolChangeFields();
        } else if ( fieldDefinition.equalsIgnoreCase( EXG_QUOTE ) ){
            fields = new ExgQuoteFields();
        } else if ( fieldDefinition.equalsIgnoreCase( TRADE ) ){
            fields = new TradeFields();
        } else if ( fieldDefinition.equalsIgnoreCase( MMQUOTE_FILE ) ) {
            fields = new MMQuoteFields();
        } else {
            System.out.println("Field type not recognized : " + fieldDefinition );
            System.exit(0);
        }
                
        Tap source = new Hfs( new TextDelimited( fields, NO_HEADER, CSV_FIELD_DELIMITER ), hdfsInputPath, SinkMode.KEEP );
        Tap sink = new Hfs( new ParquetTupleScheme(fields), hdfsOutputPath, SinkMode.REPLACE );

        Pipe pipe = new Pipe( CONVERT_PIPELINE );
        Identity identity = new Identity(fields);
        pipe = new Each(pipe, identity);

        Flow flow = connector.connect( PARQUET_DATA_FLOW, source, sink, pipe );
        flow.complete();
        
    }

}

