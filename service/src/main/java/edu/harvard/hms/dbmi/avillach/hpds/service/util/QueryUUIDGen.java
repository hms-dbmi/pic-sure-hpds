package edu.harvard.hms.dbmi.avillach.hpds.service.util;

import edu.harvard.dbmi.avillach.util.UUIDv5;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import org.springframework.stereotype.Component;

@Component
public class QueryUUIDGen {
    public void setId(Query query) {
        query.setId(""); // the id is included in the toString
        // I clear it here to keep the ID setting stable for any query
        // of identical structure and content
        String id = UUIDv5.UUIDFromString(query.toString()).toString();
        query.setId(id);
    }
}
