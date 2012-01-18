(ns stormjoin.topology
  (:use [loom.graph] :reload)
  (:use [loom.io])
  (:use [loom.alg])
  )

;(def ref g (digraph []))



(defprotocol Topology
  "Abstraction of the data structure that represents a query topology so that different data structures can be experimented with for different purposes"
  (mk-edge [source sink] "Construct and edge that can be added to an edge set")
  (add-edge-set [edge-set] "Method to add an edge, edge-set is tuples [a b] if the nodes don't exist they should be automatically added")
  (mk-node [label meta-map] "Method to create a node that can be added to an edge set")
  (add-node-set [node-set] "Method to add a set of nodes create with make-node")
  (serialize [this] "Method to serialize the topology"))


(defn -main2 [& args]
  (let [g1 (graph [1 2] [1 3] [2 3] 4)
        g2 (graph {1 [2 3] 2 [3] 4 []})
        g3 (graph g1)
        g4 (graph g3 (digraph [5 6]) [7 8] 9)
        g5 (digraph [:a :b])]
    (map #(loom.io/dot %1 (str (gensym %1) "graph.dot")) [g1 g2 g3 g4 g5]) 
    (map #(loom.io/view %1) [g1 g2 g3 g4 g5])
    ;;(map #(println % \newline) [g1 g2 g3 g4 g5])
    )
  ;;(let [g1 (graph [:workerA :workerB] [:workerA :workerC] [:workerB :workerC] :workerD)]
  ;;  (map #(println % \newline) [g1])
  ;;  (loom.io/dot g1 "g1.dot")
  ;;  )
  )

;;(testing "Construction, nodes, edges"
;;      (are [expected got] (= expected got)
;;           #{1 2 3 4} (set (nodes g1))
;;           #{[1 2] [2 1] [1 3] [3 1] [2 3] [3 2]} (set (edges g1))
;;           (set (nodes g2)) (set (nodes g1)) 
;;           (set (edges g2)) (set (edges g1))
;;           (set (nodes g3)) (set (nodes g1))
;;           (set (nodes g3)) (set (nodes g1))
;;           #{1 2 3 4 5 6 7 8 9} (set (nodes g4))
;;           #{[1 2] [2 1] [1 3] [3 1] [2 3]
;;             [3 2] [5 6] [6 5] [7 8] [8 7]} (set (edges g4))
;;             #{} (set (nodes g5))
;;             #{} (set (edges g5))
;;             true (has-node? g1 4)
;;            true (has-edge? g1 1 2)
;;            false (has-node? g1 5)
;;             false (has-edge? g1 4 1)))