(ns stormjoin.test.core
  (:use [stormjoin.core])
  (:use [clojure.test])
  (:use [loom.graph])
  (:use [stormjoin.helpers]))

(deftest test-helpers []
  ;;(is (= 0 1))
  (is (= [[1] [2] [3]] (partition-into [1 2 3] 3)))

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
  ;;(loom.io/view (breadthFirstJoinBuilder 0 3 ["A" "B" "C" "D"] [["A"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A"] ["B0" "B1"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A0 A1"] ["B0" "B1"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A"] ["B0" "B1" "B2"]]))

  ;(loom.io/view (breadthFirstPlanBuilder 0 2 ["A" "B" "C"] [["A"] ["BO" "B1"] ["C0" "C1" "C2"]]))
  ;;(def jp ("A" [["B" 3] ["C" 2] ["D" 3]] [["w1" 4] ["w2" 4]]))
  ;;(def jp (joinPlan "A" [["B" 2] ["C" 2]] [["w1" 4] ["w2" 4]]))
  ;;(println jp)
  ;;(def jp (joinPlan_g "A" [["B" 3]] [["w1" 4] ["w2" 4]]))
  ;;(println jp)
  ;;(println (createPartialJoinBolts "a AND b" [["B0" "B1"] ["C0" "C1"]]))
  )

