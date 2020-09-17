package com.equifax.api.core.tablePOJO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableFields {
    private String name;
    private String type;
    private boolean encrypt;
}
