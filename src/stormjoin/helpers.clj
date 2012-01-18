(ns stormjoin.helpers)

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