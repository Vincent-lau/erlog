-module(neg).

-include("../include/data_repr.hrl").

-compile(export_all).

-type domain() :: gb_sets:set(dl_const()).

-spec has_neg_rule(dl_program()) -> boolean().
has_neg_rule(Program) ->
  lists:any(fun(R) -> dl_repr:is_neg_rule(R) end, Program).

-spec neg_pred(dl_atom(), dl_db_instance()) -> dl_db_instance().
neg_pred(Atom, EDB) ->
  % generate all possible predicates, then subtract from the EDB
  Arity = dl_repr:get_atom_arity(Atom),
  AtomName = dl_repr:get_atom_name(Atom),
  Domain = active_domain(EDB, AtomName),
  AllAtoms = gen_all_from_domain(AtomName, Arity, Domain),
  ExistingAtoms = dbs:get_rel_by_name(AtomName, EDB),
  dbs:subtract(AllAtoms, ExistingAtoms).

-spec gen_all_from_domain(string(), integer(), domain()) -> dl_db_instance().
gen_all_from_domain(AtomName, Arity, Domain) ->
  gen_all_rec(AtomName, Arity, Domain, []).

-spec gen_all_rec(string(), integer(), domain(), [dl_const()]) -> dl_db_instance().
gen_all_rec(AtomName, Arity, _Domain, CurArgs) when length(CurArgs) == Arity ->
  dbs:singleton(
    dl_repr:cons_atom(AtomName, lists:reverse(CurArgs)));
gen_all_rec(AtomName, Arity, Domain, CurArgs) when length(CurArgs) < Arity ->
  gb_sets:fold(fun(Const, Acc) ->
                  dbs:union(Acc, gen_all_rec(AtomName, Arity, Domain, [Const | CurArgs]))
               end,
               dbs:new(),
               Domain).

%%----------------------------------------------------------------------
%% @doc
%% The active domain of a database instance I, denoted adom(I), is the set of
%% all constants occurring in I, and the active domain adom(I) of relation instance
%% I is defined analogously.
%% @end
%%----------------------------------------------------------------------
-spec active_domain(dl_db_instance(), string()) -> domain().
active_domain(EDB, AtomName) ->
  % TODO we want to find the domain for one atom or the whole db instance?
  Atoms = dbs:get_rel_by_name(AtomName, EDB),
  AllArgs = dbs:map(fun dl_repr:get_atom_args/1, Atoms),
  dbs:fold(fun(Args, Acc) -> gb_sets:union(Acc, gb_sets:from_list(Args)) end,
           gb_sets:new(),
           AllArgs).

-spec is_stratifiable(dl_program()) -> boolean().
is_stratifiable(Program) ->
  Graph = cons_pred_graph(Program),
  not check_neg_cycle(Graph).

-spec cons_pred_graph(dl_program()) -> digraph:graph().
cons_pred_graph(Program) ->
  lists:foldl(fun(Rule = #dl_rule{body = Body}, Graph) ->
                 V2 = digraph:add_vertex(Graph, dl_repr:get_rule_headname(Rule)),
                 lists:foreach(fun(Pred) ->
                                  case eval:is_idb_pred(Pred, Program) of
                                    false -> ignore;
                                    true ->
                                      V1 = digraph:add_vertex(Graph, dl_repr:get_pred_name(Pred)),
                                      digraph:add_edge(Graph, V1, V2, dl_repr:is_neg_pred(Pred))
                                  end
                               end,
                               Body),
                 Graph
              end,
              digraph:new(),
              Program).

-spec check_neg_cycle(digraph:graph()) -> boolean().
check_neg_cycle(PdGraph) ->
  Cycles = digraph_utils:cyclic_strong_components(PdGraph),
  lists:all(fun(Cycle) ->
               {_Ele, Res} =
                 lists:foldl(fun(V, {U, HasNeg}) ->
                                Out =
                                  gb_sets:from_list(
                                    digraph:out_edges(PdGraph, U)),
                                In =
                                  gb_sets:from_list(
                                    digraph:in_edges(PdGraph, V)),
                                Edges =
                                  lists:map(fun(E) -> digraph:edge(PdGraph, E) end,
                                            gb_sets:to_list(
                                              gb_sets:intersection(Out, In))),
                                AnyNegEdge =
                                  lists:any(fun({_E, _U, _V, NegEdge}) -> NegEdge end, Edges),
                                {V, HasNeg or AnyNegEdge}
                             end,
                             {lists:last(Cycle), false},
                             Cycle),
               Res
            end,
            Cycles).

%% @doc @equiv compute_stratification(anyorder, Program)
-spec compute_stratification(dl_program()) -> [dl_program()].
compute_stratification(Program) ->
  compute_stratification(anyorder, Program).

%%----------------------------------------------------------------------
%% @doc
%% We use the precedence graph to compute the stratification of the graph
%%
%% <ol>
%%  <li> find ssc of the precedence graph, treat these as strata </li>
%%  <li> perfrom top_sort on these scc, to determine the order </li>
%% </ol>
%%
%% @end
%%----------------------------------------------------------------------
-spec compute_stratification(Order, dl_program()) -> [dl_program()] | [[dl_program()]]
  when Order :: anyorder | allorder.
compute_stratification(anyorder, Program) ->
  PdGraph = cons_pred_graph(Program),
  case check_neg_cycle(PdGraph) of
    true ->
      exit(not_stratifiable);
    false ->
      PdCondensed = digraph_utils:condensation(PdGraph),
      Strata = digraph_utils:topsort(PdCondensed),
      expansion(Strata, Program)
  end;
compute_stratification(allorder, Program) ->
  PdGraph = cons_pred_graph(Program),
  case check_neg_cycle(PdGraph) of
    true ->
      exit(not_stratifiable);
    false ->
      PdCondensed = digraph_utils:condensation(PdGraph),
      topsort_all_programs(PdCondensed, Program)
  end.

%%----------------------------------------------------------------------
%% @doc
%% We wish to compute which strata can be executed concurrently.
%% We do so by inspecting the precedence graph, and take all elements with
%% no incoming edges. Remove all these nodes, and repeat this process.
%%
%% @returns a list, in which every element is a list of strata (or expanded into
%% programs) that can be executed together. The top level list preserves
%% the order in which strata should be executed.
%%
%%
%% @end
%%----------------------------------------------------------------------
-spec topsort_all(digraph:graph()) -> [[digraph:vertex()]].
topsort_all(PdGraph) ->
  case digraph:no_vertices(PdGraph) of
    N when N == 0 ->
      [];
    _N ->
      ZeroInVertices =
        lists:filter(fun(Vert) -> digraph:in_degree(PdGraph, Vert) == 0 end,
                     digraph:vertices(PdGraph)),
      true = digraph:del_vertices(PdGraph, ZeroInVertices),
      [ZeroInVertices | topsort_all(PdGraph)]
  end.

%% @doc computes a list of list of programs, where the second-level lists are strata
%% that can be independently executed.
-spec topsort_all_programs(digraph:graph(), dl_program()) -> [[dl_program()]].
topsort_all_programs(PdGraph, Program) ->
  lists:map(fun(Strata) -> expansion(Strata, Program) end, topsort_all(PdGraph)).

-spec expansion([digraph:vertex()], dl_program()) -> [dl_program()].
expansion(Strata, Program) ->
  lists:map(fun(Stratum) ->
               lists:flatmap(fun(HeadName) ->
                                lists:filter(fun(R) -> HeadName =:= dl_repr:get_rule_headname(R)
                                             end,
                                             Program)
                             end,
                             Stratum)
            end,
            Strata).
