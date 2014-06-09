(ns cascalog.in-memory.operations-test
  (:use clojure.test
        midje.sweet
        cascalog.in-memory.operations)
  (:require [cascalog.logic.vars :as v]
            [cascalog.logic.parse :as p]
            [clojure.pprint :refer (pprint)]))

(comment "playing with the cascalog query mapping"
  (let [query-map (v/with-logic-vars 
                    (p/parse-subquery [?x ?y ?z] [[[[1 2 3]] ?x] [* ?x ?x :> ?y] [* ?x ?y :> ?z]]))]
    (pprint (-> query-map
                :node
                :source
                :source 
                ))))

(deftest test-apply-transform
  (let [source-seq (seq [{"?x" 1} {"?x" 2}{"?x" 3}]) 
        map-fn (*) 
        input ["?x" "?x"]
        output ["?y"]
        ]
        (do 
          (pprint (apply-transform source-seq map-fn input output))
          (apply * [1 2])
          (pprint source-seq)
          (pprint (keys (first source-seq)))
          (pprint (get (first source-seq) "?x"))
)))
