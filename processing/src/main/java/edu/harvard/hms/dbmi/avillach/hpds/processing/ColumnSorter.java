package edu.harvard.hms.dbmi.avillach.hpds.processing;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class ColumnSorter {
    private final Map<String, Integer> infoColumnsOrder;

    @Autowired
    public ColumnSorter(@Value("#{'${variant.info_column_order:}'.split(',')}") List<String> infoColumnOrder) {
        HashMap<String, Integer> order = new HashMap<>();
        for (int i = 0; i < infoColumnOrder.size(); i++) {
            order.put(infoColumnOrder.get(i), i);
        }
        this.infoColumnsOrder = order;
    }

    public List<String> sortInfoColumns(List<String> columns) {
        // backwards compatibility check.
        if (infoColumnsOrder.isEmpty()) {
            return columns;
        }
        return columns.stream()
            .filter(infoColumnsOrder::containsKey)
            .sorted((a, b) -> Integer.compare(
                infoColumnsOrder.getOrDefault(a, Integer.MAX_VALUE),
                infoColumnsOrder.getOrDefault(b, Integer.MAX_VALUE)
            ))
            .collect(Collectors.toList());
    }
}
