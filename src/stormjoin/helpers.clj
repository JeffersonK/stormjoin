(ns stormjoin.helpers)

;;(defn- partition-into-helper [coll ngroups colls cnt]
;;  (if (empty? coll)
;;    (repeat ngroups [])
;;    (let [rem (mod cnt ngroups)
;;          (map (nth colls rem)

;;  (defn- partition-into [coll ngroups]
  ;;(let unionPartitionSize (ceil (/ (count (second stagePartitions)) unionBoltCount))
;;  (println "partition-into" coll ngroups)
;;  (if (= (count coll) ngroups)
;;    coll
;;    [(first coll) (second coll) [(nth coll 2) (nth coll 3)]]
;;    )
;;  )


(defn crossProduct [collA collB]
  """ compute the cross product """
  (for [a collA b collB]
    [a b]))

(defn assignBolt2Worker [worker bolt]
  [worker bolt])

;;debugging parts of expressions
(defmacro dbg[x] `(let [x# ~x] (println "dbg:" '~x "=" x#) x#))