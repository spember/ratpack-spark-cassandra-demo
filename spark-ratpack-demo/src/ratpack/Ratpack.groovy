import org.pember.ratpack.sparkdemo.SparkDemoModule
import org.pember.ratpack.sparkdemo.SparkDemoUrlMapping
import org.pember.ratpack.sparkdemo.config.CassandraConfig
import org.pember.ratpack.sparkdemo.config.SparkConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ratpack.func.Action
import ratpack.server.BaseDir
import ratpack.handlebars.HandlebarsModule;

import static ratpack.handlebars.Template.handlebarsTemplate;


import static ratpack.groovy.Groovy.ratpack
import static ratpack.handling.RequestLogger.ncsa

final Logger log = LoggerFactory.getLogger("ratpack")

ratpack {
    serverConfig { config ->
        maxContentLength(15 * 1048576) // 15MB
        port(5056)

        config
                .baseDir(BaseDir.find())
                .onError(Action.throwException()).yaml("config.yaml")
                .env() // override local params with incoming Environment params
                .sysProps()
                .require("/spark", SparkConfig)
                .require("/cassandra", CassandraConfig)
    }

    bindings {
        module HandlebarsModule
        module SparkDemoModule
    }

    handlers {
        all(ncsa(log))

        files { it.dir("public") }

        get {
            render(handlebarsTemplate("index.html"))
        }

        prefix("api/stocks") {
            all chain(registry.get(SparkDemoUrlMapping))
        }
    }
}

