package com.github.kkomits.opal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MarketsLoader {
    public static Market[] loadFromXml(String path) {
        List<Market> markets = new ArrayList<>();
        try {
            File xmlFile = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList nList = doc.getElementsByTagName("market");
            for (int i = 0; i < nList.getLength(); i++) {
                Node node = nList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element elem = (Element) node;
                    String symbol = elem.getElementsByTagName("symbol").item(0).getTextContent();
                    int price = Integer.parseInt(elem.getElementsByTagName("price").item(0).getTextContent());
                    int bookDepth = Integer.parseInt(elem.getElementsByTagName("book_depth").item(0).getTextContent());
                    int levelDepth = Integer.parseInt(elem.getElementsByTagName("level_depth").item(0).getTextContent());
                    markets.add(new Market(symbol, price, bookDepth, levelDepth));
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to load markets.xml: " + e.getMessage());
            System.exit(1);
        }
        return markets.toArray(new Market[0]);
    }
}
