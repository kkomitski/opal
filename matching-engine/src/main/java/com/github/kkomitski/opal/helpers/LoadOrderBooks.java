package com.github.kkomitski.opal.helpers;

import java.util.ArrayList;
import java.util.List;

import com.github.kkomitski.opal.OrderBook;
import com.github.kkomitski.opal.utils.Market;
import com.github.kkomitski.opal.utils.MarketsLoader;

public class LoadOrderBooks {
  public static OrderBook[] fromXML(String source) {
    Market[] markets = MarketsLoader.load(source);
    List<OrderBook> orderBooks = new ArrayList<>();

    for (int i = 0; i < markets.length; i++) {
      Market market = markets[i];
      // Pass the dynamic sizing parameters from XML
      orderBooks.add(new OrderBook(
        market.symbol,
        i,
        market.limitsPerBook,
        market.ordersPerLimit
      ));
    }

    return orderBooks.toArray(new OrderBook[0]);
  }
}
