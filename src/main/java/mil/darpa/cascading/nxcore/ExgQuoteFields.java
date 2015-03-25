package mil.darpa.cascading.nxcore;

import cascading.tuple.Fields;

/**
 *
 * @author Matt Parker
 */
public class ExgQuoteFields  extends Fields {

    public ExgQuoteFields(){
        super("system_date",
              "system_time",
              "system_time_zone",
              "dst_indicator",
              "number_of_days_since_1883_01_01",
              "day_of_week",
              "day_of_year",
              "session_date", 
              "session_dst_indicator",
              "session_number_of_days_since_1883_01_01",   //10
              "session_day_of_week",
              "session_day_of_year",
              "exchange_timestamp",
              "exchange_timestamp_time_zone",
              "symbol",
              "listed_exchange_index",
              "reporting_exchange_index",
              "session_id",
              "asking_price",
              "asking_price_change",        //20
              "asking_size",
              "asking_size_change",
              "bid_price",
              "bid_price_change",
              "bid_size",
              "bid_size_change",
              "nasdaq_bid_tick",
              "price_type",
              "quote_condition",
              "refresh",                    //30
              "bbo_change_flags",
              "best_ask_condition",
              "best_ask_exchange",
              "best_asking_price",
              "best_asking_price_change",
              "best_asking_size",
              "best_asking_size_change",
              "best_bid_condition",
              "best_bid_exchange",
              "best_bid_price",             //40
              "best_bid_price_change",
              "best_bid_size",
              "best_bid_size_change",
              "closing_quote_flag",
              "prevoius_best_asking_exchange",
              "previous_best_bid_exchange");  //46        
    }
    
}
