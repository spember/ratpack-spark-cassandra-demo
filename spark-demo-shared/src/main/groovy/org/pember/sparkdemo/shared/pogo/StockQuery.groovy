package org.pember.sparkdemo.shared.pogo

import groovy.transform.CompileStatic

import java.time.LocalDate

@CompileStatic
class StockQuery  implements Serializable {
    String symbol;
    LocalDate start;
}
