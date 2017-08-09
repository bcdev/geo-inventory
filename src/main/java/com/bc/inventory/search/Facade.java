package com.bc.inventory.search;

import java.io.IOException;
import java.util.List;

public interface Facade {
    
    int updateIndex(String... filenames) throws IOException;

    List<String> query(Constrain constrain) throws IOException;
    
    void dump(String csvFile) throws IOException;
}
