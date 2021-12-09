-module(dbs_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

-import(dl_repr, [cons_atom/2]).
-import(dbs, [from_list/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

% TODO consider rename
join_test_() ->
  [{"Simple joining of database instances",
    {setup, fun start/0, fun join_singleton_list/1}},
   {"Joining two lists of atoms", {setup, fun start/0, fun join_two_lists/1}},
   {"Joining relations with three args", {setup, fun start/0, fun join_3_tuples/1}},
   {"Joining on other columns", {setup, fun start/0, fun join_on_inner_cols/1}}].

project_test_() ->
  [{"Basic projection test", {setup, fun start/0, fun project_2tuples/1}},
   {"Projecting multiple cols", {setup, fun start/0, fun project_multiple/1}},
   {"Projection preserves ordering", {setup, fun start/0, fun ordered_projection/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start() ->
  ok.

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%

project_multiple(_) ->
  DB =
    dbs:from_list([#dl_atom{pred_sym = t1, args = [a, b, c, d]},
                      #dl_atom{pred_sym = t2, args = [a, d, e, f]}]),
  R = dbs:project(DB, [2, 4]),
  [?_assertEqual(dbs:from_list([#dl_atom{pred_sym = t1, args = [b, d]},
                                   #dl_atom{pred_sym = t2, args = [d, f]}]),
                 R)].

project_2tuples(_) ->
  DB =
    dbs:from_list([#dl_atom{pred_sym = t1, args = [b, c]},
                      #dl_atom{pred_sym = t2, args = [a, d]}]),
  R = dbs:project(DB, [2]),
  [?_assertEqual(dbs:from_list([#dl_atom{pred_sym = t1, args = [c]},
                                   #dl_atom{pred_sym = t2, args = [d]}]),
                 R)].

ordered_projection(_) ->
  DB = from_list([cons_atom("t1", ["a", "b", "c"])]),
  R = dbs:project(DB, [3, 1, 2]),
  ?_assertEqual(from_list([cons_atom("t1", ["c", "a", "b"])]), R).

join_singleton_list(_) ->
  Link1 = #dl_atom{pred_sym = link, args = [b, c]},
  Link2 = #dl_atom{pred_sym = link, args = [b, d]},
  Reachable = #dl_atom{pred_sym = reachable, args = [a, b]},
  Kb = dbs:from_list([Link1, Link2, Reachable]),
  Delta = dbs:join(Kb, reachable, link, [2], [1], reachable),
  [?_assertEqual(dbs:from_list([#dl_atom{pred_sym = reachable, args = [a, b, c]},
                                   #dl_atom{pred_sym = reachable, args = [a, b, d]}]),
                 Delta)].

join_two_lists(_) ->
  Link1 = #dl_atom{pred_sym = link, args = [b, c]},
  Link2 = #dl_atom{pred_sym = link, args = [c, d]},
  Reachable1 = #dl_atom{pred_sym = reachable, args = [a, b]},
  Reachable2 = #dl_atom{pred_sym = reachable, args = [a, c]},
  Kb = dbs:from_list([Link1, Link2, Reachable1, Reachable2]),
  Delta = dbs:join(Kb, reachable, link, [2], [1], reachable),
  [?_assertEqual(dbs:from_list([#dl_atom{pred_sym = reachable, args = [a, b, c]},
                                   #dl_atom{pred_sym = reachable, args = [a, c, d]}]),
                 Delta)].

join_3_tuples(_) ->
  Link1 = #dl_atom{pred_sym = link, args = [b, c, d]},
  Link2 = #dl_atom{pred_sym = link, args = [b, d, e]},
  Reachable = #dl_atom{pred_sym = reachable, args = [a, f, b]},
  Kb = dbs:from_list([Link1, Link2, Reachable]),
  Delta = dbs:join(Kb, reachable, link, [3], [1], reachable),
  [?_assertEqual(dbs:from_list([#dl_atom{pred_sym = reachable, args = [a, f, b, c, d]},
                                   #dl_atom{pred_sym = reachable, args = [a, f, b, d, e]}]),
                 Delta)].

join_on_inner_cols(_) ->
  Link1 = #dl_atom{pred_sym = link, args = [b, c, d]},
  Link2 = #dl_atom{pred_sym = link, args = [b, d, e]},
  Reachable = #dl_atom{pred_sym = reachable, args = [a, d, f]},
  Kb = dbs:from_list([Link1, Link2, Reachable]),
  Delta = dbs:join(Kb, reachable, link, [2], [3], reachable),
  [?_assertEqual(dbs:from_list([#dl_atom{pred_sym = reachable, args = [a, d, f, b, c]}]),
                 Delta)].
