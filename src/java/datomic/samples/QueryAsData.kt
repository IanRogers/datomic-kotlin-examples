package datomic.samples

import datomic.Peer
import datomic.Util
import java.io.InputStreamReader

object QueryAsData {
    fun tempid(): Any {
        return Peer.tempid(":db.part/user")
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val conn = Fns.scratchConnection()
        val url = IO.resource("datomic-java-examples/social-news.edn")
        IO.transactAll(conn, InputStreamReader(url.openStream()))

        // tx data is plain lists and maps
        conn.transact(
                listOf(mapOf(":db/id" to tempid(), ":user/firstName" to "Stewart", ":user/lastName" to "Brand"),
                        mapOf(":db/id" to tempid(), ":user/firstName" to "Stuart", ":user/lastName" to "Smalley"),
                        mapOf(":db/id" to tempid(), ":user/firstName" to "John", ":user/lastName" to "Stewart")))
        val db = conn.db()

        // Find all Stewart first names (should return 1 tuple)
        println(Peer.q("[:find ?e :in $ ?name :where [?e :user/firstName ?name]]", db, "Stewart"))

        // Find all Stewart or Stuart first names (should return 2 tuples)
        val names = listOf("Stewart", "Stuart")
        println(Peer.q("[:find ?e :in $ [?name ...] :where [?e :user/firstName ?name]]", db, names))

        // Find all [Stewart|Stuart] [first|last] names (should return 3 tuples)
        val nameAttributes = listOf(Util.read(":user/firstName"), Util.read(":user/lastName"))
        println(Peer.q("[:find ?e :in $ [?name ...] [?attr ...] :where [?e ?attr ?name]]", db, names, nameAttributes))

        // Build a query out of data.  You might need this if e.g. writing a query optimizer.
        // Do *not* do this if parameterizing inputs (as shown above) is sufficient!
        val firstNameQuery = listOf(Util.read(":find"), Util.read("?e"),
                Util.read(":in"), Util.read("$"), Util.read("?name"),
                Util.read(":where"), listOf(Util.read("?e"), Util.read(":user/firstName"), Util.read("?name")))

        // Find all Stewart first names (should return 1 tuple)
        println(Peer.q(firstNameQuery, db, "Stewart"))
    }
}