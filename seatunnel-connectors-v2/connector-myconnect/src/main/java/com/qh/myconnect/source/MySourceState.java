package com.qh.myconnect.source;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Set;

@Getter
@AllArgsConstructor
public class MySourceState implements Serializable {
    private final Set<MySourceSplit> assignedSplits;
}
