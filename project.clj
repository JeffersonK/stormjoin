(defproject stormjoin "0.1-SNAPSHOT"
  :description "parallel join planner"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.satta/loom "0.1.0-SNAPSHOT"]]
  :uberjar-exclusions [#"META-INF.*"]
  :javac-options {:debug "true" :fork "true"}
  :dev-dependencies [
                     [swank-clojure "1.2.1"]
                     [lein-ring "0.4.5"]
		     [lein-autodoc "0.9.0"]
                     ]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :extra-classpath-dirs [""]
  :aot :all
  :main stormjoin.core
  )
