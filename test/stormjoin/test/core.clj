(ns stormjoin.test.core
  (:use [stormjoin.core])
  (:use [clojure.test])
  (:use [loom.graph]))

(deftest replace-me ;; FIXME: write
  (is (= (loom.graph/digraph ["A" "SplitBolt-id3"] ["SplitBolt-id3" "B"] ["SplitBolt-id3" "C"]) (stormjoin.bolts/createSplitBolt "id3" #(> 3 %) "A" ["B" "C"]))))
  ;(is false "No tests have been written."))
