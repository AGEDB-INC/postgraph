/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


LOAD 'age';

SET search_path = ag_catalog;

SELECT * FROM create_complete_graph('gp1',5,'edges','vertices');

SELECT COUNT(*) FROM gp1."edges";
SELECT COUNT(*) FROM gp1."vertices";

SELECT * FROM cypher('gp1', $$MATCH (a)-[e]->(b) RETURN e$$) as (n agtype);

SELECT * FROM create_complete_graph('gp1',5,'edges','vertices');

SELECT COUNT(*) FROM gp1."edges";
SELECT COUNT(*) FROM gp1."vertices";

SELECT * FROM create_complete_graph('gp2',5,'edges');

SELECT * FROM create_complete_graph('gp3',5, NULL);

SELECT * FROM create_complete_graph('gp4',NULL,NULL);

SELECT * FROM create_complete_graph(NULL,NULL,NULL);

SELECT drop_graph('gp1', true);
SELECT drop_graph('gp2', true);


-- Tests for Erdos-Renyi graph generation.

-- G(n,p) model.

-- Unidirectional.
SELECT * FROM age_create_erdos_renyi_graph_gnp('ErdosRenyi_1', 6, 0, 'ERVertex', 'ER_EDGE', false); -- No edges are created.
SELECT * FROM age_create_erdos_renyi_graph_gnp('ErdosRenyi_2', 6, 1, 'ERVertex', 'ERVertex', false); -- All edges are created.

SELECT count(*) FROM "ErdosRenyi_1"."ER_EDGE"; -- No edges are created.
SELECT count(*) FROM "ErdosRenyi_2"."ER_EDGE"; -- All edges are created (15).

-- Bidirectional.
SELECT * FROM age_create_erdos_renyi_graph_gnp('ErdosRenyi_3', 6, 1, 'ERVertex', 'ER_EDGE', true); -- All edges are created.

SELECT count(*) FROM "ErdosRenyi_3"."ER_EDGE"; -- All edges are created (30).

-- Should throw errors.
SELECT * FROM age_create_erdos_renyi_graph_gnp(NULL, NULL, NULL, NULL, NULL, NULL); -- Graph name cannot be NULL.
SELECT * FROM age_create_erdos_renyi_graph_gnp('ErdosRenyi_Error', NULL, NULL, NULL, NULL, NULL); -- Number of vertices cannot be NULL.
SELECT * FROM age_create_erdos_renyi_graph_gnp('ErdosRenyi_Error', 6, NULL, NULL, NULL, NULL); -- Probability cannot be NULL.


-- G(n,m) model.

-- Unidirectional.
SELECT * FROM age_create_erdos_renyi_graph_gnm('ErdosRenyi_4', 7, 10, 'ERVertex', 'ER_EDGE', false);
SELECT * FROM age_create_erdos_renyi_graph_gnm('ErdosRenyi_5', 100, 25, 'ERVertex', 'ER_EDGE', false);

SELECT count(*) FROM "ErdosRenyi_4"."ERVertex"; -- Should return 7 vertices.
SELECT count(*) FROM "ErdosRenyi_4"."ER_EDGE"; -- Should return 10 edges.

SELECT count(*) FROM "ErdosRenyi_5"."ERVertex"; -- Should return 100 vertices.
SELECT count(*) FROM "ErdosRenyi_5"."ER_EDGE"; -- Should return 25 edges.

-- Bidirectional.
SELECT * FROM age_create_erdos_renyi_graph_gnm('ErdosRenyi_6', 7, 10, 'ERVertex', 'ER_EDGE', true);
SELECT * FROM age_create_erdos_renyi_graph_gnm('ErdosRenyi_7', 100, 25, 'ERVertex', 'ER_EDGE', true);

SELECT count(*) FROM "ErdosRenyi_6"."ERVertex"; -- Should return 7 vertices.
SELECT count(*) FROM "ErdosRenyi_6"."ER_EDGE"; -- Should return 20 edges.

SELECT count(*) FROM "ErdosRenyi_7"."ERVertex"; -- Should return 100 vertices.
SELECT count(*) FROM "ErdosRenyi_7"."ER_EDGE"; -- Should return 50 edges.

-- Should throw errors.
SELECT * FROM age_create_erdos_renyi_graph_gnm(NULL, NULL, NULL, NULL, NULL, NULL); -- Graph name cannot be NULL.
SELECT * FROM age_create_erdos_renyi_graph_gnm('ErdosRenyi_Error', NULL, NULL, NULL, NULL, NULL); -- Number of vertices cannot be NULL.
SELECT * FROM age_create_erdos_renyi_graph_gnm('ErdosRenyi_Error', 100, NULL, NULL, NULL, NULL); -- Number of edges cannot be NULL.


-- Drop Erdos-Renyi Graphs.
SELECT drop_graph('ErdosRenyi_1', true);
SELECT drop_graph('ErdosRenyi_2', true);
SELECT drop_graph('ErdosRenyi_3', true);
SELECT drop_graph('ErdosRenyi_4', true);
SELECT drop_graph('ErdosRenyi_5', true);
SELECT drop_graph('ErdosRenyi_6', true);
SELECT drop_graph('ErdosRenyi_7', true);