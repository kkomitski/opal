package com.github.kkomitski.opal.utils;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MarketsLoader {
    /**
     * Loads markets from a URL (http://...) or local file path.
     * @param source URL or file path to markets.xml
     * @return Array of Market objects
     */
    public static Market[] load(String source) {
        List<Market> markets = new ArrayList<>();
        try {
            InputStream inputStream;
            if (source.startsWith("http://") || source.startsWith("https://")) {
                URL url = new URL(source);
                inputStream = url.openStream();
            } else {
                File xmlFile = new File(source);
                inputStream = new java.io.FileInputStream(xmlFile);
            }
            
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputStream);
            doc.getDocumentElement().normalize();

            NodeList nList = doc.getElementsByTagName("market");
            for (int i = 0; i < nList.getLength(); i++) {
                Node node = nList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element elem = (Element) node;
                    String symbol = elem.getElementsByTagName("symbol").item(0).getTextContent();
                    int price = Integer.parseInt(elem.getElementsByTagName("price").item(0).getTextContent());
                    int limitsPerBook = Integer.parseInt(elem.getElementsByTagName("limits_per_book").item(0).getTextContent());
                    int ordersPerLimit = Integer.parseInt(elem.getElementsByTagName("orders_per_limit").item(0).getTextContent());
                    markets.add(new Market(symbol, price, limitsPerBook, ordersPerLimit));
                }
            }
            inputStream.close();
        } catch (Exception e) {
            System.err.println("Failed to load markets from '" + source + "': " + e.getMessage());
            System.exit(1);
        }
        return markets.toArray(new Market[0]);
    }
}
