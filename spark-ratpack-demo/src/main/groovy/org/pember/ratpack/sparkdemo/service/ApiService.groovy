package org.pember.ratpack.sparkdemo.service

import com.google.inject.Inject
import org.apache.spark.api.java.JavaSparkContext


class ApiService {

    private JavaSparkContext javaSparkContext

    @Inject ApiService(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext
    }



}
