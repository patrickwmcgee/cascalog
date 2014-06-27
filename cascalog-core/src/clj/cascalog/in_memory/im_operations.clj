(ns cascalog.in-memory.im-operations
  (:use [jackknife.seq :only (collectify)])
  (:require [cascalog.logic.vars :as v]
            [cascalog.logic.parse :as p]
            [jackknife.core :as u]))


;; Try making a function that takes a source sequence, a map fn, input field names and output field names and does the right transformation.


(defn apply-transform [source-seq mapping-fn input-names output-names]
  "For each of the source sequences apply the mapping-fn to the result of
  getting values from inputnames out of the specified source and then apply 
  the results into the output names" 
  (map (fn [source-map] 
         (let [fn-input (map (partial get source-map) input-names)
               fn-output (reduce mapping-fn fn-input)
               output-vec (collectify fn-output)]
           (if (= (count output-names) (count output-vec))
             (zipmap output-names output-vec)
             (u/throw-illegal "Output variables arity and function output arity do not match"))))
       source-seq))

(defn apply-filter-transform [source-seq input-names filter-pred]
  (filter (fn [source-map]
            (let [fn-input (map (partial get source-map) input-names)]
              (apply filter-pred fn-input)))
          source-seq))

(defn apply-mapcat-transform [source-seq input-names output-names mapcatfn]
  (mapcat (fn [source-map] 
            (let [fn-input (map (partial get source-map) input-names)
                  fn-output (apply mapcatfn fn-input)
                  output-vec (collectify fn-output)]
              (map (fn [result] 
                     (if (> 1 (count result))
                       (zipmap output-names result)
                       (assoc {} (first output-names) result)))
                   output-vec)))
          source-seq))
