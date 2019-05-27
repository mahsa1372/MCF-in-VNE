package org.ikt.spark.graphx.graphxvne

class dijkstra {
	
}

object dijkstra {
	
	type Path[Long] = (Double, List[Long])

        def Dijkstra[Long](lookup: Map[Long, List[(Double,Long)]], p: List[Path[Long]],dest: Long, visited: Set[Long]): Path[Long] = p match {
                case (dist, path) :: p_rest => path match {case key :: path_rest =>
                        if (key == dest) (dist, path.reverse)
                        else {
                                val paths = lookup(key).flatMap {case (d, key) => if (!visited.contains(key)) List((dist + d, key :: path)) else Nil}
                                val sorted_p = (paths ++ p_rest).sortWith {case ((d1, _), (d2, _)) => d1 < d2}
                                Dijkstra(lookup, sorted_p, dest, visited + key)
                        }
                }
                case Nil => (0, List())
        }
	
}
