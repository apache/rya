package org.apache.rya.joinselect;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

public class CardinalityCalcUtil {

  private static final String DELIM = "\u0000";

  private static String intToTriplePlace(int i) {

    int place = i;

    switch (place) {

      case 0:
        return "subject";

      case 1:
        return "predicate";

      case 2:
        return "object";

      default:
        throw new IllegalArgumentException("Invalid integer triple place.");

    }

  }

  private static int triplePlaceToInt(String s) {

    if (s.equals("subject")) {
      return 0;
    } else if (s.equals("predicate")) {
      return 1;
    } else if (s.equals("object")) {
      return 2;
    } else
      throw new IllegalArgumentException("Invalid triple place.");

  }

  private static List<String> getVariablePos(StatementPattern sp) {

    List<String> posList = new ArrayList<String>();
    List<Var> varList = sp.getVarList();

    for (int i = 0; i < 3; i++) {
      if (!varList.get(i).isConstant()) {
        posList.add(intToTriplePlace(i));

      }
    }

    return posList;

  }

  private static List<String> getConstantPos(StatementPattern sp) {

    List<String> posList = new ArrayList<String>();
    List<Var> varList = sp.getVarList();

    for (int i = 0; i < 3; i++) {
      if (varList.get(i).isConstant()) {
        posList.add(intToTriplePlace(i));

      }
    }

    return posList;

  }

  // assumes sp contains at most two constants
  // TODO might not be good if all variable sp is needed to get table size
  public static String getRow(StatementPattern sp, boolean joinTable) {

    String row = "";
    String values = "";
    List<Var> varList = sp.getVarList();
    List<String> constList = CardinalityCalcUtil.getConstantPos(sp);
    int i;

    for (String s : constList) {

      i = CardinalityCalcUtil.triplePlaceToInt(s);

      if (row.equals("subject") && s.equals("object") && joinTable) {
        row = s + row;
        if (values.length() == 0) {
          values = values + removeQuotes(varList.get(i).getValue().toString());
        } else {
          values = removeQuotes(varList.get(i).getValue().toString()) + DELIM + values;
        }
      } else {
        row = row + s;
        if (values.length() == 0) {
          values = values + removeQuotes(varList.get(i).getValue().toString());
        } else {
          values = values + DELIM + removeQuotes(varList.get(i).getValue().toString());
        }
      }

    }

    return (row + DELIM + values);

  }
  
  
  
  
    private static String removeQuotes(String s) {
        String trim = s.trim();
        if (trim.substring(0, 1).equals("\"")) {
            trim = trim.substring(1, trim.length() - 1);
        }
        return trim;
    }
  
  
  
  

  public static long getJCard(Key key) {

    String s = key.getColumnQualifier().toString();
    return Long.parseLong(s);

  }

  //determines a list of the positions in which two SPs have a common variable
  private static List<String> getJoinType(StatementPattern sp1, StatementPattern sp2) {

    List<String> joinList = new ArrayList<String>();
    List<Var> spList1 = sp1.getVarList();
    List<Var> spList2 = sp2.getVarList();

    List<String> pos1 = CardinalityCalcUtil.getVariablePos(sp1);
    List<String> pos2 = CardinalityCalcUtil.getVariablePos(sp2);

    int i, j;

    for (String s : pos1) {
      for (String t : pos2) {
        i = CardinalityCalcUtil.triplePlaceToInt(s);
        j = CardinalityCalcUtil.triplePlaceToInt(t);

        if (spList1.get(i).getName().equals(spList2.get(j).getName())) {
          joinList.add(s);
          joinList.add(t);

        }

      }
    }
    if (joinList.size() == 4) {
      return orderJoinType(joinList);
    }

    return joinList;

  }

  // assumes list size is four
  private static List<String> orderJoinType(List<String> jList) {

    List<String> tempList = new ArrayList<String>();

    if (jList.get(0).equals("subject") && jList.get(2).equals("object")) {
      tempList.add(jList.get(2));
      tempList.add(jList.get(0));
      tempList.add(jList.get(3));
      tempList.add(jList.get(1));
      return tempList;
    } else {
      tempList.add(jList.get(0));
      tempList.add(jList.get(2));
      tempList.add(jList.get(1));
      tempList.add(jList.get(3));
      return tempList;
    }

  }

  // assumes size is four
  private static List<String> reverseJoinType(List<String> jList) {

    List<String> tempList = new ArrayList<String>();

    if (jList.get(2).equals("subject") && jList.get(3).equals("object")) {
      tempList.add(jList.get(3));
      tempList.add(jList.get(2));
      tempList.add(jList.get(1));
      tempList.add(jList.get(0));
      return tempList;
    } else if (jList.get(2).equals("predicate") && jList.get(3).equals("subject")) {
      tempList.add(jList.get(3));
      tempList.add(jList.get(2));
      tempList.add(jList.get(1));
      tempList.add(jList.get(0));
      return tempList;
    } else if (jList.get(2).equals("object") && jList.get(3).equals("predicate")) {
      tempList.add(jList.get(3));
      tempList.add(jList.get(2));
      tempList.add(jList.get(1));
      tempList.add(jList.get(0));
      return tempList;
    } else {
      tempList.add(jList.get(2));
      tempList.add(jList.get(3));
      tempList.add(jList.get(0));
      tempList.add(jList.get(1));
      return tempList;
    }
  }

  public static List<String> getJoins(StatementPattern sp1, StatementPattern sp2) {
    List<String> jList = new ArrayList<String>();
    List<String> list = getJoinType(sp1, sp2);
    if (list.size() == 0) {
      return list;
    } else if (list.size() == 2) {
      jList.add(list.get(0) + list.get(1));
      jList.add(list.get(1) + list.get(0));
      return jList;
    } else {

      list = orderJoinType(list);
      jList.add(list.get(0) + list.get(1) + list.get(2) + list.get(3));
      jList.add(list.get(0) + list.get(1) + list.get(3) + list.get(2));
      list = reverseJoinType(list);
      jList.add(list.get(0) + list.get(1) + list.get(2) + list.get(3));
      jList.add(list.get(0) + list.get(1) + list.get(3) + list.get(2));
      return jList;
    }
  }

}
