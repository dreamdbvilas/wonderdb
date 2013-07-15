/*******************************************************************************
 *    Copyright 2013 Vilas Athavale
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package org.wonderdb.block.record.impl.base;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.wonderdb.types.DBType;
import org.wonderdb.types.impl.StringType;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class XMLValueExtractor implements RecordValueExtractor {
	
	private static XMLValueExtractor instance = new XMLValueExtractor();
	
	private XMLValueExtractor() {
	}
	
	public static XMLValueExtractor getInstance() {
		return instance;
	}

	@Override
	public DBType extract(String path, DBType dtvalue, String rootNode) {
		String value = null;
		if (dtvalue instanceof StringType) {
			value = ((StringType) dtvalue).get();
		}
		
		if (value == null) {
			return null;
		}
		
		byte[] bytes = value.getBytes();
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		InputSource is = new InputSource(bais);
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;
		Document doc = null; 
		try {
			db = factory.newDocumentBuilder();
			try {
				doc = db.parse(is);
			} catch (SAXException e) {
				return null;
			} catch (IOException e) {
				return null;
			}
		} catch (ParserConfigurationException e) {
			return null;
		}
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		XPathExpression exp = null;
		try {
			exp = xpath.compile(path);
		} catch (XPathExpressionException e) {
			return null;
		}
		Node o = null;
		try {
			if (rootNode != null && rootNode.length() > 0) {
				o = (Node) exp.evaluate(doc, XPathConstants.NODE);				
//				if (o != null) {
					TransformerFactory tf = TransformerFactory.newInstance();
					Transformer t = null;
					try {
						t = tf.newTransformer();
					} catch (TransformerConfigurationException e) {
						return null;
					}
					doc = db.newDocument();
					Node root = doc.createElement(rootNode);
					doc.appendChild(root);
					if (o != null) {
						root.appendChild(doc.adoptNode(o));
					}
					DOMSource ds = new DOMSource(doc);
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					StreamResult sr = new StreamResult(bos);
					try {
						t.transform(ds, sr);
					} catch (TransformerException e) {
						return null;
					}
					bytes = bos.toByteArray();
					value = new String(bytes);
//				}
			} else {
				value = (String) exp.evaluate(doc, XPathConstants.STRING);
			}
		} catch (XPathExpressionException e) {
			return null;
		}
		return new StringType(value);
	}

}
