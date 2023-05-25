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

#include "postgres.h"

#include "access/xact.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "commands/defrem.h"
#include "commands/schemacmds.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "parser/parser.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"
#include "utils/rel.h"

#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "commands/label_commands.h"
#include "utils/graphid.h"
#include "commands/graph_commands.h"
#include "utils/load/age_load.h"
#include "utils/load/ag_load_edges.h"
#include "utils/load/ag_load_labels.h"


// Auxiliary struct for the Erdos-Renyi G(n,m) model.
typedef struct erdos_renyi_edge
{
    graphid from_vertex;
    graphid to_vertex;
    bool chosen;
} erdos_renyi_edge;


// Function to calculate the factorial of a number.
int factorial(int num) 
{
    int result = 1;
    for (int i = 1; i <= num; i++) 
    {
        result *= i;
    }
    return result;
}


// Function to calculate the combination of two numbers (a > b).
int combination(int a, int b) 
{
    int numerator = factorial(a);
    int denominator = factorial(b) * factorial(a - b);
    int result = numerator / denominator;
    return result;
}


PG_FUNCTION_INFO_V1(create_complete_graph);

/*
* SELECT * FROM ag_catalog.create_complete_graph('graph_name',no_of_nodes, 'edge_label', 'node_label'=NULL);
*/

Datum create_complete_graph(PG_FUNCTION_ARGS)
{   
    Oid graph_id;
    Name graph_name;

    int64 no_vertices;
    int64 i,j,vid = 1, eid, start_vid, end_vid;

    Name vtx_label_name = NULL;
    Name edge_label_name;
    int32 vtx_label_id;
    int32 edge_label_id;

    agtype *props = NULL;
    graphid object_graph_id;
    graphid start_vertex_graph_id;
    graphid end_vertex_graph_id;

    Oid vtx_seq_id;
    Oid edge_seq_id;

    char* graph_name_str;
    char* vtx_name_str;
    char* edge_name_str;

    graph_cache_data *graph_cache;
    label_cache_data *vertex_cache;
    label_cache_data *edge_cache;

    Oid nsp_id;
    Name vtx_seq_name;
    char *vtx_seq_name_str;

    Name edge_seq_name;
    char *edge_seq_name_str;

    int64 lid; 

    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("graph name can not be NULL")));
    }

    if (PG_ARGISNULL(1))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("number of nodes can not be NULL")));
    }
    
    if (PG_ARGISNULL(2))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("edge label can not be NULL")));
    }


    graph_name = PG_GETARG_NAME(0);
    no_vertices = (int64) PG_GETARG_INT64(1);
    edge_label_name = PG_GETARG_NAME(2);
    namestrcpy(vtx_label_name, AG_DEFAULT_LABEL_VERTEX);

    graph_name_str = NameStr(*graph_name);
    vtx_name_str = AG_DEFAULT_LABEL_VERTEX;
    edge_name_str = NameStr(*edge_label_name);


    if (!graph_exists(graph_name_str))
    {
        DirectFunctionCall1(create_graph, CStringGetDatum(graph_name));

    }

    graph_id = get_graph_oid(graph_name_str);

    
    
    if (!PG_ARGISNULL(3))
    {
        vtx_label_name = PG_GETARG_NAME(3);
        vtx_name_str = NameStr(*vtx_label_name);
        // Check if label with the input name already exists
        if (!label_exists(vtx_name_str, graph_id))
        {
            DirectFunctionCall2(create_vlabel, CStringGetDatum(graph_name), CStringGetDatum(vtx_label_name));
        }
    }

    if (!label_exists(edge_name_str, graph_id))
    {
        DirectFunctionCall2(create_elabel, CStringGetDatum(graph_name), CStringGetDatum(edge_label_name));   
    }

    vtx_label_id = get_label_id(vtx_name_str, graph_id);
    edge_label_id = get_label_id(edge_name_str, graph_id);

    graph_cache = search_graph_name_cache(graph_name_str);
    vertex_cache = search_label_name_graph_cache(vtx_name_str,graph_id);
    edge_cache = search_label_name_graph_cache(edge_name_str,graph_id);

    nsp_id = graph_cache->namespace;
    vtx_seq_name = &(vertex_cache->seq_name);
    vtx_seq_name_str = NameStr(*vtx_seq_name);

    edge_seq_name = &(edge_cache->seq_name);
    edge_seq_name_str = NameStr(*edge_seq_name);

    vtx_seq_id = get_relname_relid(vtx_seq_name_str, nsp_id);
    edge_seq_id = get_relname_relid(edge_seq_name_str, nsp_id);

    /* Creating vertices*/
    for (i=(int64)1;i<=no_vertices;i++)
    {   
        vid = nextval_internal(vtx_seq_id, true);
        object_graph_id = make_graphid(vtx_label_id, vid);
        props = create_empty_agtype();
        insert_vertex_simple(graph_id,vtx_name_str,object_graph_id,props);
    }

    lid = vid;
    
    /* Creating edges*/
    for (i = 1;i<=no_vertices-1;i++)
    {   
        start_vid = lid-no_vertices+i;
        for(j=i+1;j<=no_vertices;j++)
        {  
            end_vid = lid-no_vertices+j;
            eid = nextval_internal(edge_seq_id, true);
            object_graph_id = make_graphid(edge_label_id, eid);

            start_vertex_graph_id = make_graphid(vtx_label_id, start_vid);
            end_vertex_graph_id = make_graphid(vtx_label_id, end_vid);

            props = create_empty_agtype();

            insert_edge_simple(graph_id, edge_name_str,
                            object_graph_id, start_vertex_graph_id,
                            end_vertex_graph_id, props);
            
        }
    }

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(age_create_erdos_renyi_graph_gnp);
/*
    ag_catalog.age_create_erdos_renyi_graph_gnm(graph_name Name, n int, p float
                                    vertex_label_name Name DEFAULT = NULL,
                                    edge_label_name Name DEAULT = NULL,
                                    bidirectional bool DEFAULT = true)
    
    The Erdős–Rényi G(n,p) model is a random graph generation model that produces 
    graphs where each edge has a fixed probability of being present or absent, 
    independently of the other edges.
*/
Datum age_create_erdos_renyi_graph_gnp(PG_FUNCTION_ARGS)
{
    Oid graph_id;
    Oid vertex_seq_id;
    Oid edge_seq_id;
    Oid nsp_id;

    graphid object_graph_id;
    graphid current_graphid; 
    graphid* vertex_array;

    agtype *props = NULL;

	Name graph_name;
    Name vertex_label_name;
    Name vertex_seq_name;
    Name edge_label_name;
    Name edge_seq_name;

	char* graph_name_str;
    char* vertex_label_str;
    char* vertex_seq_name_str;
    char* edge_label_str;
    char* edge_seq_name_str;

    graph_cache_data *graph_cache;
    label_cache_data *vertex_cache;
    label_cache_data *edge_cache;

    int64 no_vertices, eid;
    int64 vid = 1;
    int32 vertex_label_id;
    int32 edge_label_id;

    float random_prob;
    float set_prob;
    srand(time(NULL));

    bool bidirectional;


    // Retrieve the graph name (cannot be null). 
    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Graph name cannot be NULL")));
    }
    graph_name = PG_GETARG_NAME(0);
    graph_name_str = NameStr(*graph_name);


    // Check if graph exists. If it doesn't, create the graph.
    if (!graph_exists(graph_name_str))
    {
        DirectFunctionCall1(create_graph, CStringGetDatum(graph_name));
    }
    graph_id = get_graph_oid(graph_name_str);


    // Get the number of vertices.
	if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Number of vertices cannot be NULL.")));
	}
    no_vertices = PG_GETARG_INT64(1);


    // Get the probability for each edge to exist.
    if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Probability cannot be NULL.")));
	}
    set_prob = (float) PG_GETARG_FLOAT8(2);


    // Get the vertex label.
    if (PG_ARGISNULL(3)) 
    {
        namestrcpy(vertex_label_name, AG_DEFAULT_LABEL_VERTEX);
        vertex_label_str = AG_DEFAULT_LABEL_VERTEX;
    }
    else 
    {
        vertex_label_name = PG_GETARG_NAME(3);
        vertex_label_str = NameStr(*vertex_label_name);
    }


    // Get the edge label. 
    if (PG_ARGISNULL(4))
    {
        namestrcpy(edge_label_name, AG_DEFAULT_LABEL_EDGE);
        edge_label_str = AG_DEFAULT_LABEL_EDGE;
    }
    else
    {
        edge_label_name = PG_GETARG_NAME(4);
        edge_label_str = NameStr(*edge_label_name);
    }


    // Compare both edge and vertex labels (they cannot be the same).
    if (strcmp(vertex_label_str, edge_label_str) == 0)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("vertex and edge label can not be same")));
    }


    // If the vertex label does not exist, create the label.
    if (!label_exists(vertex_label_str, graph_id))
    {
        DirectFunctionCall2(create_vlabel, CStringGetDatum(graph_name), CStringGetDatum(vertex_label_name));
    }


    // If the edge label does not exist, create the label. 
    if (!label_exists(edge_label_str, graph_id))
    {
        DirectFunctionCall2(create_elabel, CStringGetDatum(graph_name), CStringGetDatum(edge_label_name));
    }


    // Get the direction of the graph.
    bidirectional = (PG_GETARG_BOOL(5) == true) ? true : false;


    vertex_label_id = get_label_id(vertex_label_str, graph_id);
    edge_label_id = get_label_id(edge_label_str, graph_id);

    graph_cache = search_graph_name_cache(graph_name_str);
    vertex_cache = search_label_name_graph_cache(vertex_label_str,graph_id);
    edge_cache = search_label_name_graph_cache(edge_label_str,graph_id);

    nsp_id = graph_cache->namespace;
    vertex_seq_name = &(vertex_cache->seq_name);
    vertex_seq_name_str = NameStr(*vertex_seq_name);

    edge_seq_name = &(edge_cache->seq_name);
    edge_seq_name_str = NameStr(*edge_seq_name);

    vertex_seq_id = get_relname_relid(vertex_seq_name_str, nsp_id);
    edge_seq_id = get_relname_relid(edge_seq_name_str, nsp_id);

    vertex_array = (graphid*) malloc(sizeof(graphid) * no_vertices);


    // Create the vertices and add them to vertex_array.
    for (int i = 0; i < no_vertices; i++)
    {
        vid = nextval_internal(vertex_seq_id, true);
        object_graph_id = make_graphid(vertex_label_id, vid);
        props = create_empty_agtype();

        // Insert the vertex in the graph and in the array.
        insert_vertex_simple(graph_id, vertex_label_str, object_graph_id, props);
        vertex_array[i] = object_graph_id;
    }


    /* For each unique pair (i, j), generate a random number. If it's probability is P or grater, create
       an edge between i and j. 
     */
    for (int i = 0; i < no_vertices; i++)
    {
        // It starts with (int j = i) because it's a combination of C(n, 2) total edges.
        for (int j = i; j < no_vertices; j++)
        {
            // Generate a random float number between 0 and 1.
            random_prob = (float) ((float)rand() / (float)RAND_MAX);

            if (random_prob <= set_prob && i != j) // (i != j) doesn't allow edges to the same node.
            {
                eid = nextval_internal(edge_seq_id, true);
                object_graph_id = make_graphid(edge_label_id, eid);

                props = create_empty_agtype();

                insert_edge_simple(graph_id, edge_label_str,
                                object_graph_id, vertex_array[i],
                                vertex_array[j], props);

                if (bidirectional == true)
                {
                    eid = nextval_internal(edge_seq_id, true);
                    object_graph_id = make_graphid(edge_label_id, eid);

                    props = create_empty_agtype();

                    insert_edge_simple(graph_id, edge_label_str,
                                    object_graph_id, vertex_array[j],
                                    vertex_array[i], props);
                }
            }
        }
    }


    free(vertex_array);
    PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(age_create_erdos_renyi_graph_gnm);
/*
    ag_catalog.age_create_erdos_renyi_graph_gnm(graph_name Name, n int, p float
                                    vertex_label_name Name DEFAULT = NULL,
                                    edge_label_name Name DEAULT = NULL,
                                    bidirectional bool DEFAULT = true)
    
    The Erdős–Rényi G(n,p) model is a random graph generation model that produces 
    graphs where each edge has a fixed probability of being present or absent, 
    independently of the other edges.
*/
Datum age_create_erdos_renyi_graph_gnm(PG_FUNCTION_ARGS)
{
    Oid graph_id;
    Oid vertex_seq_id;
    Oid edge_seq_id;
    Oid nsp_id;

    graphid object_graph_id;
    graphid current_graphid; 
    graphid* vertex_array;

    agtype *props = NULL;

	Name graph_name;
    Name vertex_label_name;
    Name vertex_seq_name;
    Name edge_label_name;
    Name edge_seq_name;

	char* graph_name_str;
    char* vertex_label_str;
    char* vertex_seq_name_str;
    char* edge_label_str;
    char* edge_seq_name_str;

    graph_cache_data *graph_cache;
    label_cache_data *vertex_cache;
    label_cache_data *edge_cache;

    int64 no_vertices, eid, no_edges;
    int64 vid = 1;
    int32 vertex_label_id;
    int32 edge_label_id;
    int total_possible_edges;

    erdos_renyi_edge* possible_edges;

    float random_prob;
    srand(time(NULL));

    bool bidirectional;


    // Retrieve the graph name (cannot be null). 
    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Graph name cannot be NULL")));
    }
    graph_name = PG_GETARG_NAME(0);
    graph_name_str = NameStr(*graph_name);


    // Check if graph exists. If it doesn't, create the graph.
    if (!graph_exists(graph_name_str))
    {
        DirectFunctionCall1(create_graph, CStringGetDatum(graph_name));
    }
    graph_id = get_graph_oid(graph_name_str);


    // Get the number of vertices. 
	if (PG_ARGISNULL(1))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Number of vertices cannot be NULL.")));
	}
    no_vertices = PG_GETARG_INT64(1);


    // Get the number of edges. 
    if (PG_ARGISNULL(2))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Probability cannot be NULL.")));
	}
    no_edges = PG_GETARG_INT64(2);


    // Get the vertex label.
    if (PG_ARGISNULL(3)) 
    {
        namestrcpy(vertex_label_name, AG_DEFAULT_LABEL_VERTEX);
        vertex_label_str = AG_DEFAULT_LABEL_VERTEX;
    }
    else 
    {
        vertex_label_name = PG_GETARG_NAME(3);
        vertex_label_str = NameStr(*vertex_label_name);
    }


    // Get the edge label. 
    if (PG_ARGISNULL(4))
    {
        namestrcpy(edge_label_name, AG_DEFAULT_LABEL_EDGE);
        edge_label_str = AG_DEFAULT_LABEL_EDGE;
    }
    else
    {
        edge_label_name = PG_GETARG_NAME(4);
        edge_label_str = NameStr(*edge_label_name);
    }


    // Compare both edge and vertex labels (they cannot be the same).
    if (strcmp(vertex_label_str, edge_label_str) == 0)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("vertex and edge label can not be same")));
    }


    // If the vertex label does not exist, create the label.
    if (!label_exists(vertex_label_str, graph_id))
    {
        DirectFunctionCall2(create_vlabel, CStringGetDatum(graph_name), CStringGetDatum(vertex_label_name));
    }


    // If the edge label does not exist, create the label. 
    if (!label_exists(edge_label_str, graph_id))
    {
        DirectFunctionCall2(create_elabel, CStringGetDatum(graph_name), CStringGetDatum(edge_label_name));
    }


    // Get the direction of the graph.
    bidirectional = (PG_GETARG_BOOL(5) == true) ? true : false;


    vertex_label_id = get_label_id(vertex_label_str, graph_id);
    edge_label_id = get_label_id(edge_label_str, graph_id);

    graph_cache = search_graph_name_cache(graph_name_str);
    vertex_cache = search_label_name_graph_cache(vertex_label_str,graph_id);
    edge_cache = search_label_name_graph_cache(edge_label_str,graph_id);

    nsp_id = graph_cache->namespace;
    vertex_seq_name = &(vertex_cache->seq_name);
    vertex_seq_name_str = NameStr(*vertex_seq_name);

    edge_seq_name = &(edge_cache->seq_name);
    edge_seq_name_str = NameStr(*edge_seq_name);

    vertex_seq_id = get_relname_relid(vertex_seq_name_str, nsp_id);
    edge_seq_id = get_relname_relid(edge_seq_name_str, nsp_id);

    total_possible_edges = combination(no_edges, 2);
    possible_edges = (erdos_renyi_edge*) malloc(sizeof(erdos_renyi_edge) * total_possible_edges);

    vertex_array = (graphid*) malloc(sizeof(graphid) * no_vertices);


    // Create the vertices and add them to vertex_array.
    for (int i = 0; i < no_vertices; i++)
    {
        vid = nextval_internal(vertex_seq_id, true);
        object_graph_id = make_graphid(vertex_label_id, vid);
        props = create_empty_agtype();

        // Insert the vertex in the graph and in the array.
        insert_vertex_simple(graph_id, vertex_label_str, object_graph_id, props);
        vertex_array[i] = object_graph_id;
    }

    // TODO: For each unique pair (i, j), add the corresponding edge to the `possible_edges` array.

    free(vertex_array);
    free(possible_edges);
    PG_RETURN_VOID();
}
