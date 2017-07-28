package org.pember.sparkdemo.shared.pogo

import groovy.transform.CompileStatic


@CompileStatic
class Company implements Serializable {
    String symbol
    String name
    String address

}
