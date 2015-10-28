package mvm.rya.accumulo.documentIndex;

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
