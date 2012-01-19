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
(defn- processStreamList  
  ([predicate Streams] (processStreamList predicate Streams 0))
  ([predicate Streams cnt] (when-not (= 0 (count Streams))
                   (let [ [streamId joinParalellism unionParallelism] (first Streams)
                          streamParts (if (= 0 cnt)
                                        (vec (map #(str streamId % ) (range joinParalellism)))
                                        (vec (map #(str "PJOIN(" predicate "=>" streamId % ")") (range joinParalellism))))
                                        ;(println "Got: " streamId ", " paralellism))
                                        ;(println "Parts: " streamParts)
                                        ]
                     (let [s (cons streamParts (processStreamList predicate (rest Streams) (+ cnt 1)))]
                       s)))
     )
  )
  
;; builds the query plan breadth first
;;
;; thisStageNo := the current stage number
;; finalStageNo := this final stage number so we know when we are done
;; stageInputs := a list of streams that are inputs to this stage
;; stagePartitions := a list of the partitions for each stage. the partitions for the current stage are at the front of the list
;; unionStageParallelism := how much to parallelize the unions in between stages. min is 1 max is the parallelization of the next stage. This doesn't apply to the first and last stages
;; returns the inputs to next stage which are the outputs at the current stage

;;TODO: if anchor stream has parallelism > 1 need to do union first before dup
;;QQQQ: is it better to replicate the union bolts in the intermediate stages or have a single one and the duplicate. the latter is less parallel but less resource intensive also
;;TODO: need a setting on whether we are optimizing for slot usage (minimize) or for speed of computation potentially much higher bandwidth do to data being replicated.
(defn- breadthFirstJoinBuilder
  ([predicate streams] (let [stream-parts (processStreamList predicate streams)]
                         (breadthFirstJoinBuilder predicate 0 (- (count streams) 1) (map #(first %) streams) stream-parts (map #(last %) streams))))
  ([predicate thisStageNo finalStageNo stageInputs stagePartitions unionStageParallelism]  
     (cond
      (= 0 thisStageNo) (let [upstream-sub-graph (breadthFirstJoinBuilder predicate (+ 1 thisStageNo) finalStageNo (first stagePartitions) (rest stagePartitions) (rest unionStageParallelism))
                              anchorStream (first stageInputs)
                              anchorPartCount (count (first stagePartitions))
                              anchorUnionBolt (if (< 1 anchorPartCount)
                                                (stormjoin.bolts/createUnionBolt anchorStream (first stagePartitions) (str "DupBolt-" anchorStream))
                                                nil)
                              dupBolt (if (< 1 anchorPartCount)
                                        (stormjoin.bolts/createDupBolt (str anchorStream) (str "UnionBolt-" anchorStream) (second stagePartitions))
                                        (stormjoin.bolts/createDupBolt (str anchorStream) anchorStream (second stagePartitions)))
                              splitBolts (map (fn [x y] (stormjoin.bolts/createSplitBolt (str x) (str predicate) y x)) (rest stagePartitions) (rest stageInputs))
                              sub-graph (apply (partial loom.graph/digraph dupBolt) splitBolts)]
                          (apply loom.graph/digraph (filter #(not (nil? %)) [sub-graph upstream-sub-graph anchorUnionBolt])))
      (< thisStageNo finalStageNo) (let [upstream-sub-graph (breadthFirstJoinBuilder predicate (+ 1 thisStageNo) finalStageNo (first stagePartitions) (rest stagePartitions) (rest unionStageParallelism))
                                         ;;TODO: abilty to specify parallelism of intermediate unions
                                         ;;unionBoltCount (min (count (second stagePartitions)) (if (< 0 (count unionStageParallelism))
                                         ;;                                                       (first unionStageParallelism)
                                         ;;                                                       (count (second stagePartitions))))                                      
                                         ;;unionPartitions (partition-into (second stagePartitions) unionBoltCount)
                                         unionBolts (map (fn [x] (stormjoin.bolts/createUnionBolt (str x) (first stagePartitions) x)) (second stagePartitions))]
                                     ;;(println unionBoltCount unionPartitions (second stagePartitions))
                                     (reduce #(loom.graph/digraph %1 %2) upstream-sub-graph unionBolts))
      (= thisStageNo finalStageNo) (stormjoin.bolts/createUnionBolt (str stagePartitions) (first stagePartitions) "*END*")
      :else (println "WTF?!!!"));;TODO: throw an error shouldn't get here
     )
  )

;;
;; anchorStream := the stream that is being duplicate to stage 1
;; streams      := list of tuples (streamId parallelism)
;; workers      := list of tuples (workerId openSlots)
(defn generateStormJoinPlan [anchorStream streams workers]
  )


(defn -main [& args]
  ;;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 3 ["A" "B" "C" "D"] [["A0" "A1"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"]] []));2 3 2]))

  (loom.io/view (breadthFirstJoinBuilder "f(x,y)" [["A" 1 1] ["B" 2 2]]))
  ;;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" [["A" 2 2] ["B" 2 2] ["C" 4 4]]))
  ;;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 (- (count stream-inputs) 1) ["A" "B" "C"] stream-parts [2 2 4]))
  ;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 4 ["A" "B" "C" "D" "E"] [["A0" "A1"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"] ["E0" "E1" "E2" "E3"]] [3 2 4]))
  ;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 3 ["A" "B" "C" "D"] [["A"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"]]))
  )


  