package mil.darpa.cascading.nxcore;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
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
    private static final String MMQUOTE_FILE = "mmquote";

    private static final boolean NO_HEADER = false;

    private static final String CSV_FIELD_DELIMITER = "\\t";
    private static final String CONVERT_PIPELINE = "convert";
    private static final String PARQUET_DATA_FLOW = "parquet-data-flow";
    private static final String CSV_TO_PARQUET_ETL = "csv-to-parquet-etl";

    private static final String EXGQUOTE_SCHEMA = "exgquote.schema";
    private static final String MMQUOTE_SCHEMA = "mmquote.schema";
    private static final String SYMBOL_CHANGE_SCHEMA = "symbolchange.schema";
    private static final String TRADE_SCHEMA = "trade.schema";
    
    /**
     * @param args the command line arguments
     */ 
    public static void main(String[] args) {

        for (int i = 0; i < 3; i++) {
            System.out.println("args[" + i + "]: " + args[i] + "\n");
        }

        String hdfsInputPath = args[0];
        String hdfsOutputPath = args[1];
        String fieldDefinition = args[2];

        Fields fields = null;
        String schema = null;
        
        if ( fieldDefinition.equalsIgnoreCase( SYMBOL_CHANGE ) ) {
            System.out.println("Setting symbol_change fields...");
            fields = new SymbolChangeFields();
            schema = readSchema( SYMBOL_CHANGE_SCHEMA );
        } else if ( fieldDefinition.equalsIgnoreCase( EXG_QUOTE ) ) {
            System.out.println("Setting exg_quote fields...");
            fields = new ExgQuoteFields();
            schema = readSchema( EXGQUOTE_SCHEMA );     
        } else if ( fieldDefinition.equalsIgnoreCase( TRADE ) ) {
            System.out.println("Setting trade fields...");
            fields = new TradeFields();
            schema = readSchema( TRADE_SCHEMA );
        } else if ( fieldDefinition.equalsIgnoreCase( MMQUOTE_FILE ) ) {
            System.out.println("Setting mm_quote fields...");
            fields = new MMQuoteFields();
            schema = readSchema( MMQUOTE_SCHEMA );
        } else {
            System.out.println("Field type not recognized : " + fieldDefinition);
            System.exit(0);
        }

        System.out.println("Schema:\n\n" + schema );
        
        Tap source = new Hfs(new TextDelimited( fields, NO_HEADER, CSV_FIELD_DELIMITER ), hdfsInputPath);
        Tap sink = new Hfs(new ParquetTupleScheme( fields, fields, schema ), hdfsOutputPath);

        Pipe convert = new Pipe( CONVERT_PIPELINE );
        Identity identity = new Identity();
        convert = new Each( convert, Fields.ALL, identity );
        
        Properties properties = AppProps.appProps()
                .setName(CSV_TO_PARQUET_ETL)
                .setVersion("1.0.0")              
                .setJarClass(CsvToParquet.class)
                .buildProperties();

        FlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect( CSV_TO_PARQUET_ETL, source, sink, convert );
        flow.complete();
        
    }

    private static String readSchema( String schemafile ){
        
        try {
            InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream( schemafile );        
            String schema = IOUtils.toString( in, "utf-8");
            System.out.println("Loaded " + schemafile );
            return schema;
        } catch ( Exception e ){
            System.out.println("Can't load " + schemafile );
            System.out.println( e.toString() );
            e.printStackTrace();
        }
        
        return "";
        
    }
    
    /*
    private static class PackThriftFunction extends BaseOperation<Tuple> implements Function<Tuple> {

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            
            TupleEntry arguments = functionCall.getArguments();
            Tuple result = new Tuple();
            
            //Name name = new Name();
            //name.setFirst_name(arguments.getString(0));
            //name.setLast_name(arguments.getString(1));
            //result.add(name);
            
            functionCall.getOutputCollector().add(result);
            
        } 
    */
       
}
