-module(naive_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

get_proj_cols_test_() ->
  [{"test getting correct projection cols", {setup, fun start/0, fun multi_proj_cols/1}}].

get_overlap_cols_test_() ->
  [{"test getting correct overlapping cols",
    {setup, fun start/0, fun multi_overlap_cols/1}}].


eval_one_rule_test_() ->
  [{"test eval once for iteration 2 of TC",
    {setup, fun start_one_iter/0, fun trans_closure_rule/1}},
   {"test eval singleton rules such as reachable(X,Y):-link(X,Y).",
    {setup, fun start_initial/0, fun singleton_rule/1}},
   {"test eval tc from start to end", {setup, fun start_initial/0, fun eval_to_end/1}}].


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

eval_to_end({Program, EDB}) ->
  FinalIDB = naive:eval_all(Program, EDB),
  [?_assertEqual(final_db(), FinalIDB)].

trans_closure_rule({[_, Rule2], EDB}) ->
  DeltaAtoms = naive:eval_one_rule(Rule2, EDB),
  db_ops:print_db(DeltaAtoms),
  [?_assertEqual(db_ops:from_list([#dl_atom{pred_sym = reachable, args = [a, c]},
                                   #dl_atom{pred_sym = reachable, args = [b, d]}]),
                 DeltaAtoms)].

singleton_rule({[R1, _], EDB}) ->
  DeltaAtoms = naive:eval_one_rule(R1, EDB),
  [?_assertEqual(db_ops:from_list([#dl_atom{pred_sym = reachable, args = [a, b]},
                                   #dl_atom{pred_sym = reachable, args = [b, c]},
                                   #dl_atom{pred_sym = reachable, args = [c, d]}]),
                 DeltaAtoms)].

multi_proj_cols(_) ->
  {Args1, Args2} = overlapping_args(),
  L = naive:get_proj_cols(Args1, Args2),
  [?_assertEqual([2, 3, 5], L)].

multi_overlap_cols(_) ->
  {Args1, Args2} = overlapping_args(),
  {L1, L2} = naive:get_overlap_cols(Args1, Args2),
  [?_assertEqual(length(L1), length(L2)),
   ?_assertEqual([1, 2, 3], L1),
   ?_assertEqual([2, 3, 5], L2)].

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%
initial_db() ->
  db_ops:from_list([#dl_atom{pred_sym = link, args = [a, b]},
                    #dl_atom{pred_sym = link, args = [b, c]},
                    #dl_atom{pred_sym = link, args = [c, d]}]).

first_iter_db() ->
  db_ops:from_list([#dl_atom{pred_sym = link, args = [a, b]},
                    #dl_atom{pred_sym = link, args = [b, c]},
                    #dl_atom{pred_sym = link, args = [c, d]},
                    #dl_atom{pred_sym = reachable, args = [a, b]},
                    #dl_atom{pred_sym = reachable, args = [b, c]},
                    #dl_atom{pred_sym = reachable, args = [c, d]}]).

final_db() ->
  db_ops:from_list([#dl_atom{pred_sym = reachable, args = [a, b]},
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
  #dl_rule{head = reachable_atom("X", "Y"), body = [link_atom("X", "Y")]}.

rule2() ->
  #dl_rule{head = reachable_atom("X", "Y"),
           body = [link_atom("X", "Z"), reachable_atom("Z", "Y")]}.

reachable_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = reachable, args = [Arg1, Arg2]}.

link_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = link, args = [Arg1, Arg2]}.

overlapping_args() ->
  Args1 = dl_repr:cons_args_from_list(["T", "Y", "L"]),
  Args2 = dl_repr:cons_args_from_list(["G", "T", "Y", "S", "L", "P"]),
  {Args1, Args2}.
