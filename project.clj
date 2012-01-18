(defproject stormjoin "0.1-SNAPSHOT"
  :description "storm query compiler and planner"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.satta/loom "0.1.0-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 ]
  :uberjar-exclusions [#"META-INF.*"]
  :javac-options {:debug "true" :fork "true"}
  :dev-dependencies [[swank-clojure "1.2.1"]
                     [lein-ring "0.4.5"]
		     [lein-autodoc "0.9.0"]
                     [com.stuartsierra/lazytest "1.1.2"]
                     [lein-autotest "1.1.0"]]
  :repositories {"stuartsierra-releases" "http://stuartsierra.com/maven2"}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :extra-classpath-dirs [""]
  :autodoc { :name "Storm Compiler", :page-title "Storm Compiler API Documentation"}
  :aot :all
  :main stormjoin.core
  )
