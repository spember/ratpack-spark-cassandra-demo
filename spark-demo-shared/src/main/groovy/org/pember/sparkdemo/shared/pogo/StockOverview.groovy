package org.pember.sparkdemo.shared.pogo

import groovy.transform.CompileStatic

@CompileStatic
class StockOverview implements Serializable {
    String symbol
    String companyName
    Double thirtyDayLow
    Double thirtyDayHigh
    Integer thirtyDayAverageVolume

    String toString() {
        "$symbol ($companyName) 30 day low: ${getThirtyDayLow()}, high: ${getThirtyDayHigh()} avg volume: ${getThirtyDayAverageVolume()}"
    }
}
