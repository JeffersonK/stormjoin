(ns stormjoin.bolts
  (:gen-class)
  (:use [loom.label])
  (:use [loom.graph] :reload))


;;
;;id     := name assigned to the bolt
;;f      := the function used to split the source stream
;;source := the source stream
;;sinks  := an ordered set of sinks (order matters) (shema same)
(defn createSplitBolt [id f source sinks]
  "create a sub-graph that represents a Split Bolt"
  (let [id (str "SplitBolt-" id)
        edges (map #(identity [id %]) sinks)
        g0 (loom.graph/digraph [source id])        
        g1 (apply loom.graph/add-edges g0 edges)
        ;;TODO: add f to the node
        ]
    g1))


;;
;;id     := name assigned to the bolt
;;source := the source stream
;;sinks  := a set of sinks (order doesn't matters) (schema same)
(defn createDupBolt [id source sinks]
  "create a sub-graph that represents a Dup Bolt"
  (let [id (str "DupBolt-" id)
        g0 (loom.graph/digraph)
        g1 (loom.graph/add-edges g0 [source id])
        g2 (apply loom.graph/add-edges g1 (map #(identity [id %]) sinks))]
    g2)
  )

;;
;;id     := name assigned to the bolt
;;sources := the source streams that have a common schema
;;sinks  := a set of sinks (order doesn't matters) (schema same)
(defn createUnionBolt [id sources sink]
  "create a sub-graph that represents a Union Bolt"
  ;;TODO: check the schemas are common
  ;;TODO: add real logging
  (if (= 1 (count sources))
    (loom.graph/digraph [(first sources) sink]) ;if there is only 1 stream to union directly connect them
    (let [id (str "UnionBolt-" id)
      g0 (loom.graph/digraph [id sink])
      g1 (apply loom.graph/add-edges g0 (map #(identity [% id]) sources))]
    g1)))


;;
;;id              := name assigned to the bolt
;;joinStream      := the stream we are joining into the topology 
;;unionStream     := stream resulting from the union of previous stage streams which all have a common schema 
;;predicate       := the join predicate which is really just a filter, this may be re-written by planner from the original predicate
;;sink            := an ordered set of sinks (order matters) (schema changes)
(defn createPartialJoinBolt [id predicate joinStream unionStream sink]
  "create a sub-graph that represents a PartialJoin (Stormjoin) Bolt"   
  (let [id (str "PartialJoinBolt-" id " [" predicate "]")
        g0 (loom.graph/digraph [joinStream id] [unionStream id] [id sink])]
    g0))

;;
;;id     := name assigned to the bolt
;;fpred  := filter predicate, tuples that match will be kept
;;source := the source stream
;;sink   := output sink (schema is the same)
(defn createFilterBolt [id fpred source sink]
  "create a sub-graph that represents a Filter Bolt"
  (let [id (str "FilterBolt-" id " [" fpred "]")
        g0 (loom.graph/digraph [source id] [id sink])]
    g0))

;;
;;id     := name assigned to the bolt
;;f      := function applied to each tuple 
;;source := the source stream
;;sink   := output sink (schema changes)
(defn createMapBolt [id mapf source sink]
  "create a sub-graph that represents a Map Bolt"
  (let [id (str "MapBolt-" id " [" mapf "]")
        g0 (loom.graph/digraph [source id] [id sink])]
    g0))

;;
;;id     := name assigned to the bolt
;;fg     := function applied to create groupings 
;;source := the source stream
;;sink   := output sink (schema changes)
(defn createGroupingBolt [id fgroup source sink]
  "create a sub-graph that represents a GroupBy Bolt"
  (let [id (str "GroupingBolt-" id " [" fgroup "]")
        g0 (loom.graph/digraph [source id] [id sink])]
    g0))

