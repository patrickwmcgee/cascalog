(ns cascalog.in-memory.im-operations
  (:require [cascalog.logic.vars :as v]
            [cascalog.logic.parse :as p]))


;; Try making a function that takes a source sequence, a map fn, input field names and output field names and does the right transformation.


(defn apply-transform [source-seq mapping-fn input-names output-names]
  "For each of the source sequences apply the mapping-fn to the result of
  getting values from inputnames out of the specified source and then apply 
  the results into the output names" 
  (map (fn [source-map] 
         (let [fn-input (map (partial get source-map) input-names)
               fn-output (reduce mapping-fn fn-input)
               output-vec (if (vector? fn-output) fn-output [fn-output])]
           (zipmap output-names output-vec)))
       source-seq))
