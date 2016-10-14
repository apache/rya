package org.apache.rya.accumulo.documentIndex;

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


import org.apache.hadoop.io.Text;

public class TextColumn  {
    
    
    private Text columnFamily;
    private Text columnQualifier;
    private boolean isPrefix = false;
    
    
    
    public TextColumn(Text columnFamily, Text columnQualifier) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
    }
    
    
    public TextColumn(TextColumn other) {
        
        this.columnFamily = new Text(other.columnFamily);
        this.columnQualifier = new Text(other.columnQualifier);
        this.isPrefix = other.isPrefix;
      
    }
    
    
    public Text getColumnFamily() {
        return columnFamily;
    }
    
    
    public boolean isPrefix() {
        return isPrefix;
    }
    
    
    public void setIsPrefix(boolean isPrefix) {
        this.isPrefix = isPrefix;
    }
    
    
    public boolean isValid() {
        return (columnFamily != null && columnQualifier != null);
    }
    
    
    
    public Text getColumnQualifier() {
        return columnQualifier;
    }
    
    
    public void setColumnFamily(Text cf) {
        this.columnFamily = cf;
    }
    
    public void setColumnQualifier(Text cq) {
        this.columnQualifier = cq;
    }
    
    public String toString() {
        
        return columnFamily.toString() + ",  " + columnQualifier.toString() + ",    prefix:" + isPrefix;
    }
    
    @Override
    public boolean equals(Object other) {
        
        if(other == null) {
            return false;
        }
        
        if(!(other instanceof TextColumn)) {
            return false;
        }
        
        TextColumn tc = (TextColumn) other;
        
        return this.columnFamily.equals(tc.columnFamily) && this.columnQualifier.equals(tc.columnQualifier) && this.isPrefix == tc.isPrefix;
        
        
        
    }
    

}
