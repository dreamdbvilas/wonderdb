package org.wonderdb.parser.jtree;

public class ParsedFilterBuilder {
	private static ParsedFilterBuilder instance = new ParsedFilterBuilder();
	
	private ParsedFilterBuilder() {
	}
	
	public static ParsedFilterBuilder getInstance() {
		return instance;
	}	
}
