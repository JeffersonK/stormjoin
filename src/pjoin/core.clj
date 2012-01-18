(ns pjoin.core
  (:gen-class)
  (:use [loom.graph] :reload)
  (:use [loom.io])
  (:use [loom.alg])
  (:use [pjoin.helpers])
  (:use [pjoin.topology])
  (:use [pjoin.bolts])
  (:use [clojure.test]))


(defn- processStreamList [Streams]
  """ based on the parallelism break create the stream part names """
  (when-not (= 0 (count Streams))
    (let [ [streamId paralellism] (first Streams)
           streamParts (vec (map #(str streamId %) (range paralellism)))]
      ;(println "Got: " streamId ", " paralellism))
      ;(println "Parts: " streamParts)
      (let [s (cons streamParts (processStreamList (rest Streams)))]
        s))))

;; builds the query plan breadth first
;;
;; thisStageNo := the current stage number
;; finalStageNo := this final stage number so we know when we are done
;; stageInputs := a list of streams that are inputs to this stage
;; stagePartitions := a list of the partitions for each stage. the partitions for the current stage are at the front of the list
;;
;; returns the inputs to next stage which are the outputs at the current stage
(defn- handleAnchor [anchorStreamPartitions]
  (if (= 1 (count anchorStreamPartitions))
    (first anchorStreamPartitions)
    (
     ;;TODO: if anchor stream has parallelism > 1 need to do union first before dup
    )))
  
(defn- breadthFirstPlanBuilder [thisStageNo finalStageNo stageInputs stagePartitions]
  (cond
   (= 0 thisStageNo) (let [upstream-sub-graph (breadthFirstPlanBuilder (+ 1 thisStageNo) finalStageNo (first stagePartitions) (rest stagePartitions))
                           anchorStream (first stageInputs)
                           dupBolt (pjoin.bolts/createDupBolt (str anchorStream) anchorStream (second stagePartitions))
                           splitBolts (map (fn [x y] (pjoin.bolts/createSplitBolt (str x) "f(RoundRobin)" y x)) (rest stagePartitions) (rest stageInputs))
                           sub-graph (apply (partial loom.graph/digraph dupBolt) splitBolts)]
                       (loom.graph/digraph sub-graph upstream-sub-graph))
   (< thisStageNo finalStageNo) (let [upstream-sub-graph (breadthFirstPlanBuilder (+ 1 thisStageNo) finalStageNo (first stagePartitions) (rest stagePartitions))
                                      unionBolts (map (fn [x] (pjoin.bolts/createUnionBolt (str x) (first stagePartitions) x)) (second stagePartitions))]
                                  (reduce #(loom.graph/digraph %1 %2) upstream-sub-graph unionBolts))
   (= thisStageNo finalStageNo) (pjoin.bolts/createUnionBolt (str stagePartitions) (first stagePartitions) "END")
   :else (println "WTF?!!!"));;TODO: throw an error shouldn't get here
  )

;;
;; anchorStream := the stream that is being duplicate to stage 1
;; streams      := list of tuples (streamId parallelism)
;; workers      := list of tuples (workerId openSlots)
(defn generatePJoinPlan [anchorStream streams workers]
  )


(defn makeStagePartitions [streams parallelism]
  )


(defn- tests1 []
  ;;(= [[:a0 :a1 :a2] [:b0 :b1]]
  ;;(println (processStreamList [[:a 3] [:b 2]]))
  ;;(loom.io/view (pjoin.bolts/createSplitBolt "id1" "f(y)" "A" ["B" "C"]))
  ;;(loom.io/view (pjoin.bolts/createDupBolt "id1" "A" ["B" "C"]))
  ;;(loom.io/view (pjoin.bolts/createUnionBolt "id1" ["A0" "A1"] "B"))
  ;;(loom.io/view (pjoin.bolts/createUnionBolt "id1" ["A0"] "B"))
  ;;(loom.io/view (pjoin.bolts/createPartialJoinBolt "id1" "f(a,b)" "A" "B1" "C"))
  ;;(loom.io/view (pjoin.bolts/createFilterBolt "id1" "f(b)" "B1" "C1")) 
  )

(defn- tests2 []
  ;;(def jp ("A" [["B" 3] ["C" 2] ["D" 3]] [["w1" 4] ["w2" 4]]))
  ;;(def jp (joinPlan "A" [["B" 2] ["C" 2]] [["w1" 4] ["w2" 4]]))
  ;;(println jp)
  ;;(def jp (joinPlan_g "A" [["B" 3]] [["w1" 4] ["w2" 4]]))
  ;;(println jp)
  ;;(println (createPartialJoinBolts "a AND b" [["B0" "B1"] ["C0" "C1"]]))
  )


(defn -main [& args]
  (tests1)

  (loom.io/view (breadthFirstPlanBuilder 0 3 ["A" "B" "C" "D"] [["A"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A"] ["B0" "B1"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A0 A1"] ["B0" "B1"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A"] ["B0" "B1" "B2"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 2 ["A" "B" "C"] [["A"] ["BO" "B1"] ["C0" "C1" "C2"]]))
  )


  