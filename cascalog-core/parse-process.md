###How parsing happens!
When queries that start with `<-` are used ( almost every query) the `defmacro <-` is called. 
This macro takes outvars and predicates and then calls `vars/with-logic-vars` on the output of `parse-subquery`.
What does `vars/with-logic-vars` do? `with-logic-vars` is a macro that "Binds all logic variables within the body of `with-logic-vars` to their string equavalent allowinging the user to write with bare symbols" 
What does `parse-subquery` do? `parse-subquery` produces a "proper subquery" by using `prepare-subquery` 
To see what these were actually doing I used the query 
`(?- (stdout) (<- [?y] ([1 2 3 4] ?x) (* ?x ?x :> ?y)))` as my test case to se what things were doing in each of the functions.
So Internally Cascalog is parsing the query into the `outvars` which are `["?y"]` 
and the `predicates` which are `(([1 2 3 4]  :> ?x) (#'clojure.core/* ?x ?x :> ?y))` a small thing to notice is that in the origional query the predicate was ([1 2 3 4] ?x) and cascalog has "desuggared" it into `([1 2 3 4] :> ?x)` the transformation on the `output-fields` and `raw-predicates` happens in `prepare-subquery`
where the `output-fields` are sanitized by `vars/sanitize` and the predicates are normalized by `predicate/normalize`

