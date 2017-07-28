package org.pember.sparkdemo.shared.pogo

import groovy.transform.CompileStatic

@CompileStatic
class RecordValueContext implements Serializable {
    String symbol
    String companyName
    Double value
    Integer shareVolume

    RecordValueContext(String symbol, String companyName, Double value, Integer shareVolume) {
        this.symbol = symbol
        this.companyName = companyName
        this.value = value
        this.shareVolume = shareVolume
    }
}
