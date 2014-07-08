(ns cascalog.in-memory.im-flow
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

(defn im-flow-def 
  "Generates an instance of ImFlowDef off of the supplied ClojureFlow."
  []
  )

(defprotocol IRunnable
  "All runnable items should implement this function."
  (run! [x]))

#_(extend-protocol IRunnable
;  IMFlowDef
;  (run! [fd]
;    (run! (compile-hadoop fd)))

  ClojureFlow
  (run! [flow]
    (run! (im-flow-def flow))))
