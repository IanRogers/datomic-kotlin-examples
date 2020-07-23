package datomic.samples

import io.kotest.core.spec.style.AnnotationSpec
import io.kotest.matchers.collections.shouldBeIn

class AnnotationSpecExample : AnnotationSpec() {
    @Test
    fun test1() {
        val graph = mapOf( // undirected links
                "a" to listOf("b"),
                "b" to listOf("a", "c", "d"),
                "c" to listOf("b", "e"),
                "d" to listOf("b", "c", "e"),
                "e" to listOf("c", "d", "f"),
                "f" to listOf("e")
        )

        Transitive.shortestPath("a", "e") {graph[it]!!} shouldBeIn listOf(
                listOf("a", "b", "c", "e"), listOf("a", "b", "d", "e")
        )
    }
}