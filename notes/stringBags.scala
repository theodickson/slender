val stringBags: RDD[(ID,Map[String,Int]),Int] //I.e. Bag[ID x [Bag[String]]]

val dslQ = For ((id,bag) <-- stringBags) Yield ((id,toK(sum(fromK(bag)))))

val q = Sum(stringBags * {(id,bag) => sng((id, toK(sum(fromK(bag)))))} )

//q returns a Bag[ID x [Int]]

val rddQ = {
    val multiplied: RDD[K,Map[(ID,Int),Int]] = stringBags.map {
        case ((id,bag)@k,v) => k -> Map((id,bag.values.reduce(_ + _)) -> v)
    }
    val flattened: RDD[(ID,Int),Int] = multiplied.values.flatten
    val summed: RDD[(ID,Int),Int] = flattened.reduceByKey(_ + _)
}


"""
Since our multiplication didnt create any maps with duplicate keys, or even more than one entry, the flatten stage just creates one row for each Map,
and the summing stage with the reduceBykey actually doesnt do any aggregations, since there is only one entry per key.
However, the goal with shredding is to flatten the inner collections, and push the computation on the inner collections into the final reduceByKey.

Generically, this will be done by creating shredding contexts, i.e. RDDs which
are distributed definitions of labels in some flat query, and when a nested
result is required, the top-level flat query is joined to the relevant shredding context, but with its constituent labels recursively renested.

This gives us back an RDD of the form:

RDD[()]
"""

val stringBagsF: RDD[(ID,Label1),Int] = stringBags.map {
    case ((id,bag),v) => ((id,Label1(id)),v) //how do we know to use id, without free variables?
}
val stringBagsCtx: RDD[Label1,Map[String,Int]] = stringBags.flatMap {
    case ((id,bag),v) => bag.shards.map { shd => Label1(id) -> shd }//sharding tuneable

// val qF: RDD[(ID,Label2),Int] = {
//     val multiplied: RDD[K,Map[(ID,Label2),Int]] = stringBagsF.map {
//         case ((id,lbl)@k,v) => k -> Map((id,Label2(lbl)) -> v)
//     }
//     val flattened: RDD[(ID,Label2),Int] = multiplied.values.flatten
//     val summed: RDD[(ID,Label2),Int] = flattened.reduceByKey(_ + _)
//     //again flatten and sum dont really do anything here, but actually, I think their existence
//     //might allow for better optimisation later
// }
// val qFCtx: RDD[Label2,Label1] =

val qF: RDD[(ID,Label1),Int] = {
    val multiplied: RDD[K,Map[(ID,Label1),Int]] = stringBags.map {
        case ((id,bag)@k,v) => k -> Map((id,Label1(id)) -> v)
    }
    val flattened: RDD[(ID,Label1),Int] = multiplied.values.flatten
    val summed: RDD[(ID,Label1),Int] = flattened.reduceByKey(_ + _)
}

val qFCtx1: RDD[Label1,Int] = stringBags.flatMap {
    case ((id,bag)@k,v) => bag.shardsmap { case (s,i) => (Label1(id),i) }
}



//imagine if there were two inner bags, this should show generically why extracting and rejoining is efficient,
//but obviously the optimal way to do just one label definition is without extracting, just flatMapping, repartitioning and reducing.