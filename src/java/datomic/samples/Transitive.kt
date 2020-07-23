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

    /*
    Given a track ID what are the IDs of the artists on that track?
     */

    private val VAET:Any = Util.read(":vaet")
    private val EAVT:Any = Util.read(":eavt")
    private val track_artists:Any = Util.read(":track/artists")
    fun datums(db: Database, index: Any, value: Any, attr: Any) : Iterable<Any> {

        // TODO move these out to an efficient place
        val REQUIRE = RT.`var`("clojure.core", "require")
        REQUIRE.invoke(Symbol.intern("datomic.api"))
        val DATOMS = RT.`var`("datomic.api", "datoms")

        val ret = DATOMS.invoke(db, index, value, attr)
        return if (ret is Iterable<*>) {
            ret.map { when (it) {
                is Datom -> it.e()
                else -> throw Exception("heck knows what this is: $it")
            } }
        } else {
            throw Exception("heck knows what this is: $ret")
        }
    }

    // dig out values from datoms
    fun artists(db: Database, id: Any) = datums(db, EAVT, id, track_artists)

    // dig out entities from datoms
    fun tracks(db: Database, id: Any) = datums(db, VAET, id, track_artists)

    @JvmStatic
    fun main(args: Array<String>) {
        val conn = Peer.connect("datomic:dev://localhost:4334/mbrainz-1968-1973")
        val db = conn.db()

        fun aid(name: String) = Peer.q("[:find ?e :in $ ?name :where [?e :artist/name ?name]]", db, name).first().first()
        val georgeharrison = aid("George Harrison")
        val dianaross = aid("Yvette Mimieux")

        println("georgeharrison: $georgeharrison")
        val tracks = tracks(db, georgeharrison)
        println("tracks of georgeharrison: $tracks")
        val artists = artists(db, tracks.first())
        println("artists of first track: $artists")

        println("artists of first track (datalog): " + Peer.q(
                "[:find ?a :in $ ?t :where [?t :track/artists ?a]]",
                db, tracks.last()
        ))

        val tracks2 = Peer.q(
                "[:find ?tracks :in $ ?artist-id :where [?tracks :track/artists ?artist-id]]",
                db, georgeharrison
        )

        fun at_neighbors(id:Any) : Iterable<Any> {
            val n1 = artists(db, id)
            return if (n1.count() != 0) {
                n1
            } else {
                tracks(db, id)
            }
        }

        val path = shortestPath(georgeharrison, dianaross, {at_neighbors(it)})
        println(path)
    }

    fun <N> shortestPath(start: N, end: N, neighbors: (N) -> Iterable<N>): List<N>? {
        val seenitstart = mutableMapOf(start to listOf(start))
        val seenitend = mutableMapOf(end to listOf(end));
        val qs = LinkedList(listOf(start))
        val qe = LinkedList(listOf(end))

        fun cons(item: N, list: List<N>): List<N> {
            // not memory efficient as we don't have clojure-like persistent data structures, but the lists won't be long
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
                return seenworking[item]!!.reversed().plus(seenother[item]!!.drop(1))
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
            if (path != null) {
                return path
            }
        }

        return null
    }

    @JvmStatic
    fun t(args: Array<String>) {
        val conn = Peer.connect("datomic:dev://localhost:4334/mbrainz-1968-1973")
        val db = conn.db()
        val rules = Util.readAll(rulessrc.reader())[0]

        println("Collaborated with...")
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

        // TODO: make a neighbors function that can bounce artist-album-artis
    }
}