package org.wonderdb.parser;


public class CreateStorageStmt {
    public String fileName;
    public String blockSize;
    public boolean isDefault = false;
    public String toString() { return "File Name: " + fileName + " Block Size: " + blockSize + " Is default: " + isDefault + "\n"; }
}

