(ns stormjoin.test.core
  (:use [stormjoin.core])
  (:use [clojure.test])
  (:use [loom.graph])
  (:use [stormjoin.helpers]))

(deftest test-helpers []
  ;;(is (= 0 1))
  (is (= [[1] [2] [3]] (partition-into [1 2 3] 3)))

  (is (= [[1 4] [2] [3]] (partition-into [1 2 3 4] 3)))

  (is (= [[1 4] [2] [3]] (partition-into [1 2 3 4] 3)))
  ;;(println (partition-into [1 2 3 4 5 6 7 8 9] 4))
  ;;(= [[:a0 :a1 :a2] [:b0 :b1]]
  ;;(println (processStreamList [[:a 3] [:b 2]]))
  )

(deftest test-bolts []
  (is (= (loom.graph/digraph ["A" "SplitBolt-id3"] ["SplitBolt-id3" "B"] ["SplitBolt-id3" "C"]) (stormjoin.bolts/createSplitBolt "id3" #(> 3 %) "A" ["B" "C"])))
  ;;(loom.io/view (stormjoin.bolts/createSplitBolt "id1" "f(y)" "A" ["B" "C"]))
  ;;(loom.io/view (stormjoin.bolts/createDupBolt "id1" "A" ["B" "C"]))
  ;;(loom.io/view (stormjoin.bolts/createUnionBolt "id1" ["A0" "A1"] "B"))
  ;;(loom.io/view (stormjoin.bolts/createUnionBolt "id1" ["A0"] "B"))
  ;;(loom.io/view (stormjoin.bolts/createPartialJoinBolt "id1" "f(a,b)" "A" "B1" "C"))
  ;;(loom.io/view (stormjoin.bolts/createFilterBolt "id1" "f(b)" "B1" "C1")) 
  )

(deftest test-join-planner []
  (is (= 0 1))

  ;;test simple join of 2 streams - no parallelism
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b)" [["A" 1 1] ["B" 1 1]] ["w1" "w2"]))

  ;;test simple join of 2 streams - parallelism of 2
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b)" [["A" 1 1] ["B" 2 2]] ["w1" "w2"]))

  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b)" [["A" 2 2] ["B" 1 1]] ["w1" "w2" "w3"])) ;; THIS CASE IS BROKEN

  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b)" [["A" 1 1] ["B" 2 2] ["C" 3 3]] ["w1" "w2" "w3"]))

  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b)" [["A" 1 1] ["B" 1 1]] ["w1" "w2" "w3"]))
  
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b,c,d)" [["A" 1 1] ["B" 2 2] ["C" 3 3] ["D" 4 4]] ["w1" "w2" "w3"]))

  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b,c,d)" [["A" 1 1] ["B" 1 1] ["C" 1 1] ["D" 1 1]] ["w1" "w2" "w3"]))

  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b,c,d)" [["A" 1 1] ["B" 2 2] ["C" 1 1] ["D" 1 1]] ["w1" "w2" "w3"]))

  ;;"pyramid" - back heavy topology
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b,c,d,e)" [["A" 1 1] ["B" 2 2] ["C" 3 3] ["D" 4 4] ["E" 5 5]] (map #(str "w" %) (range 5))))

  ;;"funnel" - front heavy topology
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b,c,d,e)" [["A" 5 5] ["B" 4 4] ["C" 3 3] ["D" 2 2] ["E" 1 1]] (map #(str "w" %) (range 5))))

  ;;"hour-glass" toplogy
  ;;(loom.io/view (stormjoin.core/genJoinPlan "f(a,b,c,d,e)" [["A" 4 4] ["B" 3 3] ["C" 1 1] ["D" 3 3] ["E" 4 4]] (map #(str "w" %) (range 5))))

  ;;"fat-waist" aka "fat-man" toplogy
  (loom.io/view (stormjoin.core/genJoinPlan "f(a,b,c,d,e)" [["A" 1 1] ["B" 3 3] ["C" 5 5] ["D" 3 3] ["E" 1 1]] (map #(str "w" %) (range 5))))
  )

