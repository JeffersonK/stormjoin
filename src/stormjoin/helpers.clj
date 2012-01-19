(ns stormjoin.helpers
  (:use [loom.graph]))

;;fire off ngroup recursive calls offset between 0 and ngroup from the beginning of the sequence
;;each recursive call will advance by nggroup down the sequence and take the first item at the point until
;; they run off the end of the sequence
;;the result is ngroup sequences that are evenly partitioned
(defn partition-into
  ([coll ngroups] (partition-into coll ngroups 0))
  ([coll ngroups cnt]
     (if (empty? coll)
       []
       (if (= cnt 0)
         (map #(partition-into (drop % coll) ngroups (+ cnt ngroups)) (range ngroups))
         (cons (first coll) (partition-into (drop ngroups coll) ngroups (+ cnt ngroups)))
         )
       )
     )
  )


;""" based on the parallelism break create the stream part names """
(defn partitionStreams  
  ([predicate Streams] (partitionStreams predicate Streams 0))
  ([predicate Streams cnt] (when-not (= 0 (count Streams))
                   (let [ [streamId joinParalellism unionParallelism] (first Streams)
                          streamParts (if (= 0 cnt)
                                        (vec (map #(str streamId % ) (range joinParalellism)))
                                        ;;TODO: this should return digraphs with only a single node rather than strings
                                        (vec (map #(str "PJoin(" predicate "," streamId % ")") (range joinParalellism))))
                                        ;(println "Got: " streamId ", " paralellism))
                                        ;(println "Parts: " streamParts)
                                        ]
                     (let [s (cons streamParts (partitionStreams predicate (rest Streams) (+ cnt 1)))]
                       s)))
     )
  )

(defn getStreamId [stream-spec]
  (first stream-spec)
  )

(defn getStreamJoinParallelism [stream-spec]
  (second stream-spec)
  )

(defn getJoinOutputParallelism [stream-spec]
  (last stream-spec)
  )

(defn collapseSubGraphs [sub-graph-coll]
  (let [sub-graph-coll-prime (filter #(not (nil? %)) sub-graph-coll)]
    (apply loom.graph/digraph (filter #(not (nil? %)) sub-graph-coll-prime)))) ;;(reduce #(loom.graph/digraph %1 %2) (loom.graph/digraph []) sub-graph-coll-prime)))


(defn unravel [n coll] 
  (map #(take-nth n (drop % coll)) (range n))) 

(defn crossProduct [collA collB]
  """ compute the cross product """
  (for [a collA b collB]
    [a b]))

(defn assignBolt2Worker [worker bolt]
  [worker bolt])

;;debugging parts of expressions
(defmacro dbg[x] `(let [x# ~x] (println "dbg:" '~x "=" x#) x#))