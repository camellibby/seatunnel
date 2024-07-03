package org.apache.seatunnel.transform.replace;


import lombok.Data;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class XjReplaceConfig implements Serializable {
    private static final long serialVersionUID = -1L;
    private String columnName;
    private String oldString = "";
    private String newString = "";
    private Integer valueIndex;
    private boolean regex;
}
