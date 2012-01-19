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
  (loom.io/view (stormjoin.core/breadthFirstJoinBuilder "f(x,y)" [["A" 1 1] ["B" 1 1]]))

    ;;test simple join of 2 streams - parallelism of 2
  (loom.io/view (stormjoin.core/breadthFirstJoinBuilder "f(x,y)" [["A" 1 1] ["B" 2 2]]))

  
  ;;(loom.io/view (breadthFirstJoinBuilder 0 3 ["A" "B" "C" "D"] [["A"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"]]))

  ;;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A"] ["B0" "B1"]]))

  ;;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A0 A1"] ["B0" "B1"]]))

  ;;(loom.io/view (breadthFirstPlanBuilder 0 1 ["A" "B"] [["A"] ["B0" "B1" "B2"]]))

  ;;(loom.io/view (breadthFirstPlanBuilder 0 2 ["A" "B" "C"] [["A"] ["BO" "B1"] ["C0" "C1" "C2"]]))

  ;;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 3 ["A" "B" "C" "D"] [["A0" "A1"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"]] []));2 3 2]))

  ;;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" [["A" 2 2] ["B" 2 2] ["C" 4 4]]))
  ;;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 (- (count stream-inputs) 1) ["A" "B" "C"] stream-parts [2 2 4]))
  ;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 4 ["A" "B" "C" "D" "E"] [["A0" "A1"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"] ["E0" "E1" "E2" "E3"]] [3 2 4]))
  ;;(loom.io/view (breadthFirstJoinBuilder "f(x,y)" 0 3 ["A" "B" "C" "D"] [["A"] ["BO" "B1"] ["C0" "C1" "C2" "C3"] ["D0" "D1"]]))
  )

