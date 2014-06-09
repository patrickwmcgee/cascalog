(ns cascalog.in-memory.flow
  (:require [jackknife.core :as u]
            [jackknife.seq :refer (unweave)]
            [cascalog.logic.algebra :refer (sum)]))

;; This is used for building flows for the cascalog in memory planner
;; a flow is a sequence of function maps of field name -> value 

;;(defn flow-def [{map-to-be-processed}]
   ;; define what a flow for the clojure in memory planner is 
  ;; define some sort of file input as a tap consctruct / generator
  ;; define outputting/sinking -> sequences OR if specified files?(as specified) 

 ;; )

