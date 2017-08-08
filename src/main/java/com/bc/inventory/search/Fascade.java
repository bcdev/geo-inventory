package com.bc.inventory.search;

import java.io.IOException;
import java.util.List;

public interface Fascade {
    int updateIndex(String... filenames) throws IOException;

    List<String> query(Constrain constrain) throws IOException;
}
