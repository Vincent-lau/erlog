-module(dbs_tests).

-include_lib("eunit/include/eunit.hrl").


-import(dl_repr, [cons_atom/2, cons_const/1]).
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
   {"Joining on other columns", {setup, fun start/0, fun join_on_inner_cols/1}},
   {"Joining acting as AND", {setup, fun start/0, fun join_as_and/1}}].

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
    dbs:from_list([cons_atom("t1", ["a", "b", "c", "d"]),
                   cons_atom("t2", ["a", "d", "e", "f"])]),
  R = dbs:project(DB, [2, 4]),
  [?_assertEqual(dbs:from_list([cons_atom("t1", ["b", "d"]),
                                cons_atom("t2", ["d", "f"])]),
                 R)].

project_2tuples(_) ->
  DB =
    dbs:from_list([cons_atom("t1", ["b", "c"]),
                   cons_atom("t2", ["a", "d"])]),
  R = dbs:project(DB, [2]),
  [?_assertEqual(dbs:from_list([cons_atom("t1", ["c"]),
                                cons_atom("t2", ["d"])]),
                 R)].

ordered_projection(_) ->
  DB = from_list([cons_atom("t1", ["a", "b", "c"])]),
  R = dbs:project(DB, [3, 1, 2]),
  ?_assertEqual(from_list([cons_atom("t1", ["c", "a", "b"])]), R).

join_singleton_list(_) ->
  Link1 = cons_atom("link", ["b", "c"]),
  Link2 = cons_atom("link", ["b", "d"]),
  Reachable = cons_atom("reachable", ["a", "b"]),
  DB1 = dbs:from_list([Link1, Link2]),
  DB2 = dbs:from_list([Reachable]),
  Delta = dbs:join(DB2, DB1, [2], [1], cons_const("reachable")),
  [?_assertEqual(dbs:from_list([cons_atom("reachable", ["a", "b", "c"]),
                                cons_atom("reachable", ["a", "b", "d"])]),
                 Delta)].

join_two_lists(_) ->
  Link1 = cons_atom("link", ["b", "c"]),
  Link2 = cons_atom("link", ["b", "d"]),
  Reachable1 = cons_atom("reachable", ["a", "b"]),
  Reachable2 = cons_atom("reachable", ["a", "c"]),
  DB1 = dbs:from_list([Link1, Link2]),
  DB2 = dbs:from_list([Reachable1, Reachable2]),
  Delta = dbs:join(DB2, DB1, [2], [1], cons_const("reachable")),
  [?_assert(dbs:equal(dbs:from_list([cons_atom("reachable", ["a", "b", "c"]),
                                cons_atom("reachable", ["a", "b", "d"])]),
                 Delta))].

join_3_tuples(_) ->
  Link1 = cons_atom("link", ["b", "c", "d"]),
  Link2 = cons_atom("link", ["b", "d", "e"]),
  Reachable = cons_atom("reachable", ["a", "f", "b"]),
  DB1 = dbs:from_list([Link1, Link2]),
  DB2 = dbs:from_list([Reachable]),
  Delta = dbs:join(DB2, DB1, [3], [1], cons_const("reachable")),
  [?_assertEqual(dbs:from_list([cons_atom("reachable", ["a", "f", "b", "c", "d"]),
                                cons_atom("reachable", ["a", "f", "b", "d", "e"])]),
                 Delta)].

join_on_inner_cols(_) ->
  Link1 = cons_atom("link", ["b", "c", "d"]),
  Link2 = cons_atom("link", ["b", "d", "e"]),
  Reachable = cons_atom("reachable", ["a", "d", "f"]),
  DB1 = dbs:from_list([Link1, Link2]),
  DB2 = dbs:from_list([Reachable]),
  Delta = dbs:join(DB2, DB1, [2], [3], cons_const("reachable")),
  [?_assertEqual(dbs:from_list([cons_atom("reachable", ["a", "d", "f", "b", "c"])]),
                 Delta)].

join_as_and(_) ->
  Links = [
    dl_repr:cons_atom("link", ["a", "a"]),
    dl_repr:cons_atom("link", ["a", "c"]),
    dl_repr:cons_atom("link", ["a", "d"]),
    dl_repr:cons_atom("link", ["b", "a"]),
    dl_repr:cons_atom("link", ["b", "b"]),
    dl_repr:cons_atom("link", ["b", "d"]),
    dl_repr:cons_atom("link", ["c", "a"]),
    dl_repr:cons_atom("link", ["c", "b"]),
    dl_repr:cons_atom("link", ["d", "a"]),
    dl_repr:cons_atom("link", ["d", "b"]),
    dl_repr:cons_atom("link", ["d", "c"]),
    dl_repr:cons_atom("link", ["d", "d"])
  ],

  Reachables = [
    dl_repr:cons_atom("reachable", ["a", "b"]),
    dl_repr:cons_atom("reachable", ["b", "c"]),
    dl_repr:cons_atom("reachable", ["c", "c"]),
    dl_repr:cons_atom("reachable", ["c", "d"])
  ],
  DB1 = dbs:from_list(Links),
  DB2 = dbs:from_list(Reachables),
  Joined = dbs:join(DB2, DB1, [1, 2], [1, 2], cons_const("both")),
  ?_assertEqual(dbs:from_list([]),Joined).
