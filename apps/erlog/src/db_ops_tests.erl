-module(db_ops_tests).

-include_lib("eunit/include/eunit.hrl").

-include("data_repr.hrl").

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
   {"Projecting multiple cols", {setup, fun start/0, fun project_multiple/1}}].


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
    [#dl_atom{pred_sym = "t1", args = [a, b, c, d]},
     #dl_atom{pred_sym = "t2", args = [a, d, e, f]}],
  R = db_ops:project(DB, [2, 4]),
  [?_assertEqual([#dl_atom{pred_sym = "t1", args = [b, d]},
                  #dl_atom{pred_sym = "t2", args = [d, f]}],
                 R)].

project_2tuples(_) ->
  DB = [#dl_atom{pred_sym = "t1", args = [b, c]}, #dl_atom{pred_sym = "t2", args = [a, d]}],
  R = db_ops:project(DB, [2]),
  [?_assertEqual([#dl_atom{pred_sym = "t1", args = [c]},
                  #dl_atom{pred_sym = "t2", args = [d]}],
                 R)].

join_singleton_list(_) ->
  Link1 = #dl_atom{pred_sym = "link", args = [b, c]},
  Link2 = #dl_atom{pred_sym = "link", args = [b, d]},
  Reachable = #dl_atom{pred_sym = "reachable", args = [a, b]},
  Kb = [Link1, Link2, Reachable],
  Delta = db_ops:join(Kb, "reachable", "link", 2, 1, "reachable"),
  [?_assertEqual([#dl_atom{pred_sym = "reachable", args = [a, b, c]},
                  #dl_atom{pred_sym = "reachable", args = [a, b, d]}],
                 Delta)].

join_two_lists(_) ->
  Link1 = #dl_atom{pred_sym = "link", args = [b, c]},
  Link2 = #dl_atom{pred_sym = "link", args = [c, d]},
  Reachable1 = #dl_atom{pred_sym = "reachable", args = [a, b]},
  Reachable2 = #dl_atom{pred_sym = "reachable", args = [a, c]},
  Kb = [Link1, Link2, Reachable1, Reachable2],
  Delta = db_ops:join(Kb, "reachable", "link", 2, 1, "reachable"),
  [?_assertEqual([#dl_atom{pred_sym = "reachable", args = [a, b, c]},
                  #dl_atom{pred_sym = "reachable", args = [a, c, d]}],
                 Delta)].

join_3_tuples(_) ->
  Link1 = #dl_atom{pred_sym = "link", args = [b, c, d]},
  Link2 = #dl_atom{pred_sym = "link", args = [b, d, e]},
  Reachable = #dl_atom{pred_sym = "reachable", args = [a, f, b]},
  Kb = [Link1, Link2, Reachable],
  Delta = db_ops:join(Kb, "reachable", "link", 3, 1, "reachable"),
  [?_assertEqual([#dl_atom{pred_sym = "reachable", args = [a, f, b, c, d]},
                  #dl_atom{pred_sym = "reachable", args = [a, f, b, d, e]}],
                 Delta)].

join_on_inner_cols(_) ->
  Link1 = #dl_atom{pred_sym = "link", args = [b, c, d]},
  Link2 = #dl_atom{pred_sym = "link", args = [b, d, e]},
  Reachable = #dl_atom{pred_sym = "reachable", args = [a, d, f]},
  Kb = [Link1, Link2, Reachable],
  Delta = db_ops:join(Kb, "reachable", "link", 2, 3, "reachable"),
  [?_assertEqual([#dl_atom{pred_sym = "reachable", args = [a, d, f, b, c]}], Delta)].
