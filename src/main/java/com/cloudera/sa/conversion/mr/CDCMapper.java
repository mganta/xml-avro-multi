package com.cloudera.sa.conversion.mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CDCMapper extends
		Mapper<LongWritable, BytesWritable, Text, MapWritable> {
	private XMLInputFactory inputFactory;
	private static final String RECORD = "record";
	private static final String NAME = "table";

	@Override
	protected void setup(Context context) {
		inputFactory = XMLInputFactory.newInstance();
	}

	@Override
	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		String outKey = null;
		MapWritable outMap = null;
		try {
			XMLEventReader eventReader = inputFactory.createXMLEventReader(new ByteArrayInputStream(value.getBytes()));
			XMLEvent event = null;
			while (eventReader.hasNext()) {
				  event = eventReader.nextEvent();

				if (event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					// If we have a item element we create a new item
					if (startElement.getName().getLocalPart().equals(RECORD)) {
						outMap = new MapWritable();
						// We read the attributes from this tag and add the date
						// attribute to our object
						Iterator<Attribute> attributes = startElement.getAttributes();
						while (attributes.hasNext()) {
							Attribute attribute = attributes.next();
							if (attribute.getName().toString().equals("DATE")) {
								outMap.put(new Text("DATE".getBytes()), new BytesWritable(attribute.getValue().getBytes()));
							}
						}
					}
					if (event.isStartElement()) {
						String elementName = event.asStartElement().getName()
								.getLocalPart();
						if (!elementName.equals("record")) {
							event = eventReader.nextEvent();
							outMap.put(new Text(elementName.getBytes()), new BytesWritable(event.asCharacters().getData().getBytes()));
							if (elementName.equals(NAME))
								outKey = event.asCharacters().getData();
						}
					}
				}
				
				if (event.isEndElement()) {
					EndElement endElement = event.asEndElement();
					if (endElement.getName().getLocalPart().equals(RECORD)) {
						context.write(new Text(outKey), outMap);
						return;
					}
				}

			}
			eventReader.close();
		} catch (XMLStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
