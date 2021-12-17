-module(eval_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

-import(dl_repr, [cons_atom/2, cons_rule/2, cons_args_from_list/1, get_rule_headname/1]).
-import(dbs, [from_list/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

get_proj_cols_test_() ->
  [{"test getting correct projection cols", {setup, fun start/0, fun multi_proj_cols/1}},
   {"test swapping column order", {setup, fun start/0, fun swap_cols/1}}].

get_overlap_cols_test_() ->
  [{"test getting correct overlapping cols",
    {setup, fun start/0, fun multi_overlap_cols/1}}].


is_idb_pred_test_() ->
  {"testing finding edb predicates", {setup, fun start_initial/0, fun find_correct_edb_pred/1}}.

eval_one_rule_test_() ->
  [{"test eval once for iteration 2 of TC",
    {setup, fun start_one_iter/0, fun trans_closure_rule/1}},
   {"test eval singleton rules such as reachable(X,Y):-link(X,Y).",
    {setup, fun start_initial/0, fun singleton_rule/1}},
   {"test eval tc from start to end", {setup, fun start_initial/0, fun eval_to_end/1}},
   {"test eval join of multiple overlapping cols",
    {setup, fun start/0, fun eval_multi_overlapping_cols/1}},
   {"test eval join of no overlapping columns",
    {setup, fun start/0, fun eval_no_overlapping_cols/1}},
   {"test eval join of complete overlapping columns",
    {setup, fun start/0, fun eval_complete_overlapping_cols/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start() ->
  ok.

start_initial() ->
  EDB = initial_db(),
  Program = tc_prog(),
  {Program, EDB}.

start_one_iter() ->
  {tc_prog(), first_iter_db()}.

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%



find_correct_edb_pred({Program, _EDB}) ->
  EDBProg = eval:get_edb_program(Program),
  ?_assertEqual([rule1()], EDBProg).

eval_to_end({Program, EDB}) ->
  FinalIDB = eval:eval_all(Program, EDB),
  ?_assertEqual(final_db(), FinalIDB).

trans_closure_rule({Prog = [_, Rule2], EDB}) ->
  DeltaAtoms = eval:eval_one_rule(Rule2, Prog, EDB),
  {F, _R} = preproc:lex_and_parse(
    "
      reachable(a, c).
      reachable(b, d). 
    "
  ),
  Facts = dbs:from_list(F),
  [?_assertEqual(Facts,
                 DeltaAtoms)].

singleton_rule({Prog = [R1, _], EDB}) ->
  DeltaAtoms = eval:eval_one_rule(R1, Prog, EDB),
  [?_assertEqual(dbs:from_list([#dl_atom{pred_sym = reachable, args = [a, b]},
                                #dl_atom{pred_sym = reachable, args = [b, c]},
                                #dl_atom{pred_sym = reachable, args = [c, d]}]),
                 DeltaAtoms)].

multi_proj_cols(_) ->
  {Args1, Args2} = overlapping_args(),
  L = eval:get_proj_cols(Args1, Args2),
  [?_assertEqual([2, 3, 5], L)].

swap_cols(_) ->
  Args1 = cons_args_from_list(["X", "Y"]),
  Args2 = cons_args_from_list(["Y", "X"]),
  L = eval:get_proj_cols(Args1, Args2),
  ?_assertEqual([2, 1], L).

multi_overlap_cols(_) ->
  {Args1, Args2} = overlapping_args(),
  {L1, L2} = eval:get_overlap_cols(Args1, Args2),
  [?_assertEqual(length(L1), length(L2)),
   ?_assertEqual([1, 2, 3], L1),
   ?_assertEqual([2, 3, 5], L2)].

eval_multi_overlapping_cols(_) ->
  R = cons_rule(cons_atom("R", ["X", "Y", "L", "G", "T", "S", "P"]),
                [cons_atom("R1", ["X", "Y", "L"]),
                 cons_atom("R2", ["G", "T", "Y", "S", "L", "P"])]),
  DB =
    dbs:from_list([cons_atom("R1", ["a", "b", "c"]),
                   cons_atom("R2", ["d", "f", "b", "e", "c", "g"]),
                   cons_atom("R2", ["d", "f", "b", "e", "b", "g"])]),
  NewDB = eval:eval_one_rule(R, [R], DB),
  Ans =
    dbs:from_list([cons_atom(get_rule_headname(R), ["a", "b", "c", "d", "f", "e", "g"])]),

  [?_assertEqual(Ans, NewDB)].

eval_no_overlapping_cols(_) ->
  R = cons_rule(cons_atom("R", ["M", "N", "P", "Q"]),
                [cons_atom("R1", ["M", "N"]), cons_atom("R2", ["P", "Q"])]),
  DB =
    dbs:from_list([cons_atom("R1", ["a", "b"]),
                   cons_atom("R1", ["c", "d"]),
                   cons_atom("R2", ["e", "f"])]),
  NewDB = eval:eval_one_rule(R, [R], DB),
  Ans =
    dbs:from_list([cons_atom(get_rule_headname(R), ["a", "b", "e", "f"]),
                   cons_atom(get_rule_headname(R), ["c", "d", "e", "f"])]),
  ?_assertEqual(Ans, NewDB).

eval_complete_overlapping_cols(_) ->
  R = cons_rule(cons_atom("P", ["X", "Y"]),
                [cons_atom("Q", ["X", "Y"]), cons_atom("R", ["X", "Y"])]),
  DB =
    dbs:from_list([cons_atom("Q", ["a", "b"]),
                   cons_atom("Q", ["b", "c"]),
                   cons_atom("R", ["b", "c"])]),
  NewDB = eval:eval_one_rule(R,[R], DB),
  Ans = dbs:from_list([cons_atom("P", ["b", "c"])]),
  ?_assertEqual(Ans, NewDB).

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%
initial_db() ->
  dbs:from_list([#dl_atom{pred_sym = link, args = [a, b]},
                 #dl_atom{pred_sym = link, args = [b, c]},
                 #dl_atom{pred_sym = link, args = [c, d]}]).

first_iter_db() ->
  dbs:from_list([#dl_atom{pred_sym = link, args = [a, b]},
                 #dl_atom{pred_sym = link, args = [b, c]},
                 #dl_atom{pred_sym = link, args = [c, d]},
                 #dl_atom{pred_sym = reachable, args = [a, b]},
                 #dl_atom{pred_sym = reachable, args = [b, c]},
                 #dl_atom{pred_sym = reachable, args = [c, d]}]).

final_db() ->
  dbs:from_list([#dl_atom{pred_sym = reachable, args = [a, b]},
                 #dl_atom{pred_sym = reachable, args = [b, c]},
                 #dl_atom{pred_sym = reachable, args = [c, d]},
                 #dl_atom{pred_sym = reachable, args = [a, c]},
                 #dl_atom{pred_sym = reachable, args = [b, d]},
                 #dl_atom{pred_sym = reachable, args = [a, d]},
                 #dl_atom{pred_sym = link, args = [a, b]},
                 #dl_atom{pred_sym = link, args = [b, c]},
                 #dl_atom{pred_sym = link, args = [c, d]}]).

tc_prog() ->
  [rule1(), rule2()].

rule1() ->
  cons_rule(reachable_atom("X", "Y"), [link_atom("X", "Y")]).

rule2() ->
  cons_rule(reachable_atom("X", "Y"), [link_atom("X", "Z"), reachable_atom("Z", "Y")]).

reachable_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = reachable, args = [Arg1, Arg2]}.

link_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = link, args = [Arg1, Arg2]}.

overlapping_args() ->
  Args1 = cons_args_from_list(["T", "Y", "L"]),
  Args2 = cons_args_from_list(["G", "T", "Y", "S", "L", "P"]),
  {Args1, Args2}.
