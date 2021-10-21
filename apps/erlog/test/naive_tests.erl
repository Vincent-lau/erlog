-module(naive_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

eval_one_rule_test_() ->
  [{"test eval once for iteration 2 of TC",
    {setup, fun start_one_iter/0, fun trans_closure_rule/1}},
   {"test eval singleton rules such as reachable(X,Y):-link(X,Y).",
    {setup, fun start_initial/0, fun singleton_rule/1}},
   {"test eval tc from start to end", {setup, fun start_initial/0, fun eval_to_end/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

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
  [?_assertEqual(db_ops:from_list([#dl_atom{pred_sym = reachable, args = [a, c]},
                                   #dl_atom{pred_sym = reachable, args = [b, d]}]),
                 DeltaAtoms)].

singleton_rule({[R1, _], EDB}) ->
  DeltaAtoms = naive:eval_one_rule(R1, EDB),
  [?_assertEqual(db_ops:from_list([#dl_atom{pred_sym = reachable, args = [a, b]},
                                   #dl_atom{pred_sym = reachable, args = [b, c]},
                                   #dl_atom{pred_sym = reachable, args = [c, d]}]),
                 DeltaAtoms)].

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
