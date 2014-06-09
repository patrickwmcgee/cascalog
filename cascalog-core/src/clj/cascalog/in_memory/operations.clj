(ns cascalog.in-memory.operations
  (:require [cascalog.logic.vars :as v]
            [cascalog.logic.parse :as p]))


;; Try making a function that takes a source sequence, a map fn, input field names and output field names and does the right transformation.


(defn apply-transform [source-seq mapping-fn input-names output-names]
  "For each of the source sequences apply the mapping-fn to the result of
  getting values from inputnames out of the specified source and then apply 
  the results into the output names" 
  (loop [output-map {}]
    (if (not (= nil (first source-seq)))
      (recur (assoc my-map (first source-seq))
             (rest source-seq))
      output-map)))
