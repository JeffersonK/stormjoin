(ns stormjoin.core
  (:gen-class)
  (:use [loom.graph] :reload)
  (:use [loom.io])
  (:use [loom.alg])
  (:use [stormjoin.helpers])
  (:use [stormjoin.topology])
  (:use [stormjoin.bolts])
  (:use [clojure.test])
  (:use [clojure.contrib.math]))


;""" based on the parallelism break create the stream part names """
(defn- partitionStreams  
  ([predicate Streams] (partitionStreams predicate Streams 0))
  ([predicate Streams cnt] (when-not (= 0 (count Streams))
                   (let [ [streamId joinParalellism unionParallelism] (first Streams)
                          streamParts (if (= 0 cnt)
                                        (vec (map #(str streamId % ) (range joinParalellism)))
                                        (vec (map #(str "PJOIN(" predicate "=>" streamId % ")") (range joinParalellism))))
                                        ;(println "Got: " streamId ", " paralellism))
                                        ;(println "Parts: " streamParts)
                                        ]
                     (let [s (cons streamParts (partitionStreams predicate (rest Streams) (+ cnt 1)))]
                       s)))
     )
  )
  
;; builds the query plan breadth first
;;
;; anchorStream := the stream that is being duplicated to stage 1
;; thisStageNo := the current stage number
;; finalStageNo := this final stage number so we know when we are done
;; stageInputs := a list of streams that are inputs to this stage
;; stagePartitions := a list of the partitions for each stage. the partitions for the current stage are at the front of the list
;; joinOuputParallelism := how much to parallelize the unions in between stages. min is 1 max is the parallelization of the next stage. This doesn't apply to the first and last stages there will be n-1 one in this list
;; returns the inputs to next stage which are the outputs at the current stage

;;TODO: if anchor stream has parallelism > 1 need to do union first before dup
;;QQQQ: is it better to replicate the union bolts in the intermediate stages or have a single one and the duplicate. the latter is less parallel but less resource intensive also
;;TODO: need a setting on whether we are optimizing for slot usage (minimize) or for speed of computation potentially much higher bandwidth do to data being replicated.
(defn- getStreamId [stream-spec]
  (first stream-spec)
  )

(defn- getStreamJoinParallelism [stream-spec]
  (second stream-spec)
  )

(defn- getJoinOutputParallelism [stream-spec]
  (last stream-spec)
  )

(defn- collapseSubGraphs [sub-graph-coll]
  (let [sub-graph-coll-prime (filter #(not (nil? %)) sub-graph-coll)]
    (apply loom.graph/digraph (filter #(not (nil? %)) sub-graph-coll-prime)))) ;;(reduce #(loom.graph/digraph %1 %2) (loom.graph/digraph []) sub-graph-coll-prime)))

;;TODO: make the joinPlanner a protocol so we can plugin different planners easily
(defn- breadthFirstJoinBuilder
  ;;case 1 - first call aka init
  ([predicate anchorStream other-streams] (let [all-streams
                                                (cons anchorStream other-streams)
                                                stream-parts
                                                (partitionStreams predicate all-streams)]
                                            (breadthFirstJoinBuilder predicate 0 (count other-streams) (map getStreamId all-streams) stream-parts (map getJoinOutputParallelism other-streams)))
     )
  ;;case 2 - recursive call
  ([predicate thisStageNo finalStageNo stageInputs stagePartitions joinOutputParallelism]  
     (cond
      (= 0 thisStageNo)
      (let [upstream-sub-graph
            (breadthFirstJoinBuilder predicate (+ 1 thisStageNo) finalStageNo (first stagePartitions) (rest stagePartitions) joinOutputParallelism)
            anchorStream
            (first stageInputs)
            anchorPartCount
            (count (first stagePartitions))
            anchorUnionBolt
            (if (< 1 anchorPartCount)
              (stormjoin.bolts/createUnionBolt anchorStream (first stagePartitions) (str "DupBolt-" anchorStream))
              nil)
            dupBolt
            (if (< 1 anchorPartCount) ;;if the anchor stream has more than 1 input stream we need to do a union of the sources first
              (stormjoin.bolts/createDupBolt (str anchorStream) (str "UnionBolt-" anchorStream) (second stagePartitions))
              (stormjoin.bolts/createDupBolt (str anchorStream) anchorStream (second stagePartitions)))
            splitBolts
            (map (fn [src sink] (stormjoin.bolts/createSplitBolt (str sink) (str predicate) src sink)) (rest stageInputs) (rest stagePartitions))
            sub-graph
            (apply (partial loom.graph/digraph dupBolt) splitBolts)]        
        (collapseSubGraphs [sub-graph upstream-sub-graph anchorUnionBolt]))
      (< thisStageNo finalStageNo)
      (let [upstream-sub-graph
            (breadthFirstJoinBuilder predicate (+ 1 thisStageNo) finalStageNo (first stagePartitions) (rest stagePartitions) (rest joinOutputParallelism))
            ;;TODO: abilty to specify parallelism of intermediate unions
            ;;unionBoltCount (min (count (second stagePartitions)) (if (< 0 (count unionStageParallelism))
            ;;                                                       (first unionStageParallelism)
            ;;                                                       (count (second stagePartitions))))                                      
            ;;unionPartitions (partition-into (second stagePartitions) unionBoltCount)
            unionBolts (map (fn [x] (stormjoin.bolts/createUnionBolt (str x) (first stagePartitions) x)) (second stagePartitions))]
        ;;(println unionBoltCount unionPartitions (second stagePartitions))
        (collapseSubGraphs (cons upstream-sub-graph unionBolts)))
      (= thisStageNo finalStageNo) (stormjoin.bolts/createUnionBolt (str stagePartitions) (first stagePartitions) "*END*")
      :else (println "WTF?!!!"));;TODO: throw an error shouldn't get here
     )
  )


;;TODO intelligently choose the anchor stream, by default breadFirstJoinBuilder will use the first stream as the anchor
(defn- chooseAnchorStream [predicate streams]
  "return a tuple of anchorStream and the other streams"
  [(first streams) (rest streams)]
  )

(defn- assignBoltToWorker [bolt worker]
  )
  
(defn- optimizeJoinPlan [optimizationFunction joinPlan workers]
  joinPlan
  )

;;

;; streams      := list of tuples (streamId parallelism)
;; TODO: replace steram definition with a real data structure. Currently is just a triple "streamid, join parallelism, distribution parallelism"
;; workers      := list of tuples (workerId openSlots)
(defn genJoinPlan [predicate streams workers]
  (println predicate streams workers)
  (let [[anchorStream other-streams] (chooseAnchorStream predicate streams)
        joinPlan (breadthFirstJoinBuilder predicate anchorStream other-streams)
        joinPlanPrime (optimizeJoinPlan #(identity %) joinPlan workers)
        ]
    joinPlanPrime
    )  
  )

(defn -main [& args]
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(x,y)" [["A" 1 1] ["B" 2 2] ["C" 3 3] ["D" 4 4]] ["w1" "w2" "w3"]))
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(x,y)" [["A" 1 1] ["B" 1 1] ["C" 1 1] ["D" 1 1]] ["w1" "w2" "w3"]))
  (loom.io/view (stormjoin.core/genJoinPlan "f(x,y)" [["A" 1 1] ["B" 2 2] ["C" 1 1] ["D" 1 1]] ["w1" "w2" "w3"]))
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(x,y)" [["A" 1 1] ["B" 2 2] ["C" 3 3]] ["w1" "w2" "w3"]))
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(x,y)" [["A" 1 1] ["B" 1 1]] ["w1" "w2" "w3"]))
  )


  