package datomic.samples

import clojure.lang.RT
import clojure.lang.Symbol
import datomic.Database
import datomic.Datom
import datomic.Peer
import datomic.Util
import java.util.*

/*
A rewrite of parts of https://hashrocket.com/blog/posts/using-datomic-as-a-graph-database but in
Kotlin and using the mbrainz dataset and rules https://github.com/Datomic/mbrainz-sample
 */
object Transitive {
    val rulessrc = """
        [[(track-release ?t ?r) [?m :medium/tracks ?t] [?r :release/media ?m]]
         [(track-info ?t ?track-name ?artist-name ?album ?year)
          [?t :track/name ?track-name]
          [?t :track/artists ?a]
          [?a :artist/name ?artist-name]
          (track-release ?t ?r)
          [?r :release/name ?album]
          [?r :release/year ?year]]
         [(short-track ?a ?t ?len ?max)
          [?t :track/artists ?a]
          [?t :track/duration ?len]
          [(< ?len ?max)]]
         [(track-search ?q ?track)
          [(fulltext ${'$'} :track/name ?q) [[?track ?tname]]]]
         [(transitive-net-1 ?attr ?a1 ?a2)
          [?x ?attr ?a1]
          [?x ?attr ?a2]
          [(!= ?a1 ?a2)]]
         [(transitive-net-2 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-2 ?attr ?a1 ?a2)
          (transitive-net-1 ?attr ?a1 ?x)
          (transitive-net-1 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-3 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-3 ?attr ?a1 ?a2)
          (transitive-net-2 ?attr ?a1 ?x)
          (transitive-net-2 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-4 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-4
           ?attr ?a1 ?a2)
          (transitive-net-3 ?attr ?a1 ?x)
          (transitive-net-3 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-5 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-5 ?attr ?a1 ?a2)
          (transitive-net-4 ?attr ?a1 ?x)
          (transitive-net-4 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-6 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-6 ?attr ?a1 ?a2)
          (transitive-net-5 ?attr ?a1 ?x)
          (transitive-net-5 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-7 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-7 ?attr ?a1 ?a2)
          (transitive-net-6 ?attr ?a1 ?x)
          (transitive-net-6 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-8 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-8 ?attr ?a1 ?a2)
          (transitive-net-7 ?attr ?a1 ?x)
          (transitive-net-7 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-9 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-9 ?attr ?a1 ?a2)
          (transitive-net-8 ?attr ?a1 ?x)
          (transitive-net-8 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(transitive-net-10 ?attr ?a1 ?a2) (transitive-net-1 ?attr ?a1 ?a2)]
         [(transitive-net-10 ?attr ?a1 ?a2)
          (transitive-net-9 ?attr ?a1 ?x)
          (transitive-net-9 ?attr ?x ?a2)
          [(!= ?a1 ?a2)]]
         [(collab ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-1 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-1 ?artist-name-1 ?artist-name-2)
          (collab ?artist-name-1 ?artist-name-2)]
         [(collab-net-2 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-2 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-3 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-3 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-4 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-4 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-5 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name
           ?artist-name-1]
          (transitive-net-5 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-6 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-6 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-7 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-7 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-8 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-8 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-9 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-9 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]
         [(collab-net-10 ?artist-name-1 ?artist-name-2)
          [?a1 :artist/name ?artist-name-1]
          (transitive-net-10 :track/artists ?a1 ?a2)
          [?a2 :artist/name ?artist-name-2]]]
    """.trimIndent()

    val REQUIRE = RT.`var`("clojure.core", "require")
    val DATOMS by lazy {
        REQUIRE.invoke(Symbol.intern("datomic.api"))
        RT.`var`("datomic.api", "datoms")
    }

    val VAET = Util.read(":vaet")
    val EAVT = Util.read(":eavt")
    val TRACK_ARTISTS = Util.read(":track/artists")
    val ARTIST_NAME = Util.read(":artist/name")
    val TRACK_NAME = Util.read(":track/name")

    // Some utility functions to access the datomic store directly
    //

    fun datoms(db: Database, index: Any, value: Any, attr: Any) : Iterable<Datom> {
        val ret = DATOMS.invoke(db, index, value, attr)
        if (ret !is Iterable<*>) {
            throw Exception("This should be a Iterable<Datom> but it's not: $ret")
        }
        return ret as Iterable<Datom>
    }
    fun attr(db: Database, id: Any, attr: Any) = datoms(db, EAVT, id, attr).map {it.v()}.firstOrNull()

    // Given an artist ID what are the IDs of the tracks they were involved with
    fun artists(db: Database, id: Any) = datoms(db, EAVT, id, TRACK_ARTISTS).map {it.v()}
    // Given a track ID what are the IDs of the artists on that track
    fun tracks(db: Database, id: Any) = datoms(db, VAET, id, TRACK_ARTISTS).map {it.e()}
    // if id is an artist then get their tracks; if it's a track then get the artists
    fun at_neighbors(db: Database, id: Any) = artists(db, id).ifEmpty { tracks(db, id) }
    // What is then name value of an item (track or artist)
    fun item_name(db: Database, id: Any) = attr(db, id, ARTIST_NAME) ?: attr(db, id, TRACK_NAME)

    /*
    A bi-directional breadth-first search for shortest path. Uses the 'visted' maps to hold the partial paths.
     */
    fun <N> shortestPath(start: N, end: N, neighbors: (N) -> Iterable<N>): List<N> {
        val seenitstart = mutableMapOf(start to listOf(start))
        val seenitend = mutableMapOf(end to listOf(end));
        val qs = LinkedList(listOf(start))
        val qe = LinkedList(listOf(end))

        fun cons(item: N, list: List<N>): List<N> {
            // not memory efficient as we don't easily have clojure-like persistent data structures, but the lists won't be long
            val coll = ArrayList<N>(list.size + 1)
            coll.add(item)
            coll.addAll(list)
            return coll
        }

        fun takestep(
                qworking: LinkedList<N>,
                seenworking: MutableMap<N, List<N>>,
                seenother: MutableMap<N, List<N>>
        ) : List<N>? {
            val item = qworking.pop()
            if (seenother.contains(item)) {
                return seenitstart[item]!!.reversed().plus(seenitend[item]!!.drop(1))
            }
            val itempath = seenworking[item]!!
            for (next in neighbors(item)) {
                if (! seenworking.containsKey(next)) {
                    qworking.add(next)
                    seenworking[next] = cons(next, itempath)
                }
            }

            return null
        }

        while (qs.isNotEmpty() && qe.isNotEmpty()) {
            val path = if (qs.size < qe.size) { // work on the side with the smallest branching first
                takestep(qs, seenitstart, seenitend)
            } else {
                takestep(qe, seenitend, seenitstart)
            }

            if (path != null) return path // found a path, we're done
        }

        return emptyList() // never found a path...
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val conn = Peer.connect("datomic:dev://localhost:4334/mbrainz-1968-1973")
        val db = conn.db()
        val rules = Util.readAll(rulessrc.reader())[0]

        println("Using 'transitive net' rules")
        println("Collaborated with, 1 step away...")
        println(Peer.q(
                "[:find ?aname ?aname2 :in $ % ?aname :where (collab ?aname ?aname2)]",
                db, rules, "George Harrison"
        ))
        println("Collaborated with, up to 2 steps away...")
        println(Peer.q(
                "[:find ?aname ?aname2 :in $ % ?aname :where (collab-net-2 ?aname ?aname2)]",
                db, rules, "George Harrison"
        ))
        println("Collaborated with, up to 3 steps away...")
        println(Peer.q(
                "[:find ?aname ?aname2 :in $ % ?aname :where (collab-net-3 ?aname ?aname2)]",
                db, rules, "George Harrison"
        ))

        println("Using a double-ended shortest-path algorithm")
        fun artist_id(name: String) = Peer.q("[:find ?e :in $ ?name :where [?e :artist/name ?name]]", db, name).first().first()
        val george_harrison = artist_id("George Harrison")
        val yvette_mimieux = artist_id("Yvette Mimieux")

        println("George Harrison: $george_harrison")
        println("Yvette Mimieux: $yvette_mimieux")

        val path = shortestPath(george_harrison, yvette_mimieux, {at_neighbors(db, it)})
        println(path)
        println(path.map {item_name(db, it)})
    }
}