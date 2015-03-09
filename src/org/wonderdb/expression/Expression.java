package org.wonderdb.expression;

import org.wonderdb.parser.jtree.UQLParserTreeConstants;

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


public interface Expression {
	public final int OR = UQLParserTreeConstants.JJTOR;
	public final int AND = UQLParserTreeConstants.JJTAND;
	public final int EQ = UQLParserTreeConstants.JJTEQ;
	public final int LT = UQLParserTreeConstants.JJTLT;
	public final int LE = UQLParserTreeConstants.JJTLE;
	public final int GT = UQLParserTreeConstants.JJTGT;
	public final int GE = UQLParserTreeConstants.JJTGE;
	public final int NE = UQLParserTreeConstants.JJTNE;
	public final int IN = 9;
	public final int LIKE = 10;
}
