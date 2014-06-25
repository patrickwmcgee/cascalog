(ns cascalog.in-memory.im-operations-test
  (:use clojure.test
        midje.sweet
        cascalog.in-memory.im-operations)
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
        map-fn *
        input ["?x" "?x"]
        output ["?y"]
        result-seq (seq [{"?y" 1} {"?y" 4} {"?y" 9}])]
    (fact
      "apply-transform properly applies a mapping transformation and returns a result sequence as a map"
      (apply-transform source-seq map-fn input output) => result-seq)))

(defn my-square [x1 x2] [x1 (* x1 x2)])
(deftest test-apply-transform-identity
  (let [source-seq (seq [{"?x" 1} {"?x" 2}{"?x" 3}])
        map-fn my-square
        input ["?x" "?x"]
        output ["?x" "?y"]
        result-seq (seq [{"?x" 1 "?y" 1} {"?x" 2 "?y" 4} {"?x" 3 "?y" 9}])]
    (fact
      "apply-transform properly applies a mapping transformation and returns a result sequence as a map for functions with multiple returns"
      (apply-transform source-seq map-fn input output) => result-seq)))

(deftest test-apply-transform-bad-arity
  (let [source-seq (seq [{"?x" 1} {"?x" 2}{"?x" 3}])
        map-fn *
        input ["?x" "?x"]
        output ["?y" "?y"]
        result-seq (seq [{"?y" 1} {"?y" 4} {"?y" 9}])]
    (fact
      "Supplying a function that returns a different number of output variables than the desired output varaibles should throw an error"
     (apply-transform source-seq map-fn input output) => (throws IllegalArgumentException))))

(deftest test-apply-filter 
  (let [source-seq (seq [{"?x" 1}{"?x" 2}{"?x" 3}
                         {"?x" 4}{"?x" 5}{"?x" 6}])
        input ["?x"]
        filter-pred even?
        result-seq (seq [{"?x" 2} {"?x" 4}{"?x" 6}])]
    (fact
      "suplying a predicate function should properly filter a sequence of data"
      (apply-filter-transform source-seq input filter-pred) => result-seq)))

(deftest test-apply-filter-other-fn
  (let [source-seq (seq [{"?x" 1}{"?x" 2}{"?x" 3}
                         {"?x" 4}{"?x" 5}{"?x" 6}])
        input ["?x"]
        filter-pred (fn [x] (> x 3))
        result-seq (seq [{"?x" 4} {"?x" 5}{"?x" 6}])]
    (fact
      "suplying a predicate function should properly filter a sequence of data"
      (apply-filter-transform source-seq input filter-pred) => result-seq)))
