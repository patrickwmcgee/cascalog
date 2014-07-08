(ns cascalog.in-memory.im-types
  (:require [jackknife.core :as u]))

;; This protocol is used for transforming some representation into a tuple
;; Duplicate of cascalog.cascading.types/ITuple
(defprotocol ITuple
  (to-tuple [this]
    "Returns a tupled representation of the supplied thing."))

(extend-protocol ITuple
  ;; If it is already a tuple, then just return the tuple?
  ;; Tuple
  ;; (to-tuple [t] t)

  ;; If the supplied type is a clojure.lang.IPersistentVector coerce it into a tuple
  ;; clojure.lang.IPersistentVector
  ;; (to-tuple [v] (Util/coerceToTuple v)) ;; TODO: do this in clojure.

  ;; This almost recursive looking definition seems fishy, it takes an Object and calls 
  ;; to-tuple twice?
  Object
  (to-tuple [v] (to-tuple [v])))

;; ## Generator Protocol
(defprotocol IGenerator
  "Accepts some type and returns a IMFlow that can be used as a
  generator. The idea is that an in-memory flow can always be used
  directly as a generator."
  (generator [x]))

(defn generator?
  "Returns true if the supplied item can be used as a Cascalog
  generator, false otherwise."
  [x]
  (satisfies? IGenerator x))

;; source-map is a map of identifier to tap, or source. 

(defrecord IMFlow [source-map sink-map tails name]
  IGenerator
  (generator [x] x))

;; Extend the protocol to create IMFlows from Vectors, Sequences, Subqueries and Arraylists
#_(extend-protocol IGenerator
  Subquery
  (generator [sq]
    (generator (.getCompiledSubquery sq)))

  clojure.lang.IPersistentVector
  (generator [v] (generator (or (seq v) ())))

  ;; This should be easier since we're going to mainly be taking sequences
  ;; clojure.lang.ISeq
  ;; (generator [v] (generator (MemorySourceTap. (map to-tuple v) Fields/ALL)))

  java.util.ArrayList
  (generator [coll]
    (generator (into [] coll))))
