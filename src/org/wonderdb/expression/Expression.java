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
package org.wonderdb.expression;


public interface Expression {
	public final int OR = 1;
	public final int AND = 2;
	public final int EQ = 3;
	public final int LT = 4;
	public final int LE = 5;
	public final int GT = 6;
	public final int GE = 7;
	public final int NE = 8;
	public final int IN = 9;
	public final int LIKE = 10;
}
