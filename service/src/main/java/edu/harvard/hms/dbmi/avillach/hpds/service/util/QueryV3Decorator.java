package edu.harvard.hms.dbmi.avillach.hpds.service.util;

import edu.harvard.dbmi.avillach.util.UUIDv5;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.Query;
import edu.harvard.hms.dbmi.avillach.hpds.processing.VariantUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class QueryV3Decorator {
    private static final Logger LOG = LoggerFactory.getLogger(QueryV3Decorator.class);

    public void setId(Query query) {
        throw new RuntimeException("Not yet implemented");
    }

    public void mergeFilterFieldsIntoSelectedFields(Query query) {
        throw new RuntimeException("Not yet implemented");
    }
}
