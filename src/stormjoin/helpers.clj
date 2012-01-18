(ns stormjoin.helpers)

(defn crossProduct [collA collB]
  """ compute the cross product """
  (for [a collA b collB]
    [a b]))

(defn assignBolt2Worker [worker bolt]
  [worker bolt])

;;debugging parts of expressions
(defmacro dbg[x] `(let [x# ~x] (println "dbg:" '~x "=" x#) x#))