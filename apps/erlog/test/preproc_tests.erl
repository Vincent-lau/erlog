-module(preproc_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

collect_args_test_() ->
  [{"test basic combinging multiple atoms",
    {setup, fun start/0, fun stop/1, fun combine_two/1}}].

rule_part_test_() ->
  [{"test partition rule with three body atoms",
    {setup, fun start2/0, fun stop/1, fun part_three/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start() ->
  ets:new(table, [named_table, public]),
  A1 = dl_repr:cons_atom("c", ["X", "Y"]),
  A2 = dl_repr:cons_atom("d", ["Y", "Z"]),
  [A1, A2].

start2() ->
  ets:new(table, [named_table, public]),
  A1 = dl_repr:cons_atom("a", ["X", "Y", "Z"]),
  A2 = dl_repr:cons_atom("b", ["W", "X"]),
  A3 = dl_repr:cons_atom("c", ["X", "Y"]),
  A4 = dl_repr:cons_atom("d", ["Y", "Z"]),
  dl_repr:cons_rule(A1, [A2, A3, A4]).

stop(_) ->
  ets:delete(table).

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%

combine_two(Atoms) ->
  R = preproc:collect_args(Atoms),
  [?_assertEqual(["X", "Y", "Z"],
                 dl_repr:get_atom_args(
                   dl_repr:get_rule_head(R)))].

part_three(R) ->
  NewR = preproc:rule_part(R),
  [?_assertEqual(part_res(), NewR)].

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

part_res() ->
  A1 = dl_repr:cons_atom("a", ["X", "Y", "Z"]),
  A2 = dl_repr:cons_atom("b", ["W", "X"]),
  A3 = dl_repr:cons_atom("_name0", ["X", "Y", "Z"]),
  A4 = dl_repr:cons_atom("c", ["X", "Y"]),
  A5 = dl_repr:cons_atom("d", ["Y", "Z"]),
  R1 = dl_repr:cons_rule(A1, [A2, A3]),
  R2 = dl_repr:cons_rule(A3, [A4, A5]),
  [R1, R2].
