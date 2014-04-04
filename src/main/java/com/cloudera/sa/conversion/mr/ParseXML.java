package com.cloudera.sa.conversion.mr;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class ParseXML {
    static final String DATE = "date";
    static final String ITEM = "record";
    static final String MODE = "mode";
    static final String UNIT = "unit";
    static final String CURRENT = "current";
    static final String INTERACTIVE = "interactive";

    @SuppressWarnings({ "unchecked", "null" })
    public List<String> readConfig(String configFile) {
        List<String> items = new ArrayList<String>();
        try {
            // First create a new XMLInputFactory
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            // Setup a new eventReader
            InputStream in = new FileInputStream(configFile);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
            // Read the XML document
            StringBuilder item = null;
           Map fields = null;

            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();

                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    // If we have a item element we create a new item
                    if (startElement.getName().getLocalPart() == (ITEM)) {
                        item = new StringBuilder();
                        fields = new HashMap<String, Object>();
                        // We read the attributes from this tag and add the date
                        // attribute to our object
                        Iterator<Attribute> attributes = startElement
                                .getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = attributes.next();
                            if (attribute.getName().toString().equals(DATE)) {
                            	item.append(" date: " + attribute.getValue());
                            }
                        }
                    }

                    if (event.isStartElement()) {
                    	    String elementName = event.asStartElement().getName().getLocalPart();
                    	    System.out.println("start element name :" + elementName);
                            if(!elementName.equals("records")) {
                            event = eventReader.nextEvent();
                            item.append(elementName + ": " +  event.asCharacters().getData());
                            fields.put(elementName, event.asCharacters().getData());
                            }

                    }   
                }
                // If we reach the end of an item element we add it to the list
                if (event.isEndElement()) {
                    EndElement endElement = event.asEndElement();
                    System.out.println("end element name :" + endElement.getName().getLocalPart());
                    if (endElement.getName().getLocalPart() == (ITEM)) {
                        items.add(item.toString());
                    }
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return items;
    }
    
    public static void main(String args[]) {
        ParseXML read = new ParseXML();
        List<String> readConfig = read.readConfig("/Users/madhu/sf/sample.xml");
        for (String item : readConfig) {
            System.out.println(item);
        }
    }
}