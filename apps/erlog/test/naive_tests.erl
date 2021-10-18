-module(naive_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

eval_one_rule_test_() ->
  [{"test eval once for iteration 2 of TC",
    {setup, fun start_one_iter/0, fun trans_closure_rule/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start_initial() ->
  EDB =
    [#dl_atom{pred_sym = "link", args = [a, b]},
     #dl_atom{pred_sym = "link", args = [b, c]},
     #dl_atom{pred_sym = "link", args = [c, d]}],
  Program =
    [#dl_rule{head = #dl_atom{pred_sym = "reachable", args = ["X", "Z"]},
              body =
                [#dl_atom{pred_sym = "link", args = ["X", "Y"]},
                 #dl_atom{pred_sym = "link", args = ["Y", "Z"]}]},
     #dl_rule{head = #dl_atom{pred_sym = "reachable", args = ["X", "Y"]},
              body = [#dl_atom{pred_sym = "link", args = ["X", "Y"]}]}],
  {Program, EDB}.

start_one_iter() ->
  {tc_prog(),
   [#dl_atom{pred_sym = "link", args = [a, b]},
    #dl_atom{pred_sym = "link", args = [b, c]},
    #dl_atom{pred_sym = "link", args = [c, d]},
    #dl_atom{pred_sym = "reachable", args = [a, b]},
    #dl_atom{pred_sym = "reachable", args = [b, c]},
    #dl_atom{pred_sym = "reachable", args = [c, d]}]}.

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%

eval_to_end({Program, EDB}) ->
  FinalIDB = naive:eval_all(Program, EDB),
  [?_assertEqual(final_db(), FinalIDB)].

trans_closure_rule({[_, Rule2], EDB}) ->
  utils:dbg_format("~p~n", [EDB]),
  DeltaAtoms = naive:eval_one_rule(Rule2, EDB),
  [?_assertEqual([#dl_atom{pred_sym = "reachable", args = [a, c]},
                  #dl_atom{pred_sym = "reachable", args = [b, d]}],
                 DeltaAtoms)].

singleton_rule(EDB) ->
  Reachable = #dl_atom{pred_sym = "reachable", args = ["X", "Y"]},
  Link = #dl_atom{pred_sym = "link", args = ["X", "Y"]},
  Rule = #dl_rule{head = Reachable, body = [Link]},
  DeltaAtoms = naive:eval_one_rule(Rule, EDB),
  [?_assertEqual([#dl_atom{pred_sym = "reachable", args = [a, b]},
                  #dl_atom{pred_sym = "reachable", args = [b, c]},
                  #dl_atom{pred_sym = "reachable", args = [c, d]}],
                 DeltaAtoms)].

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

final_db() ->
  [#dl_atom{pred_sym = "reachable", args = [a, b]},
   #dl_atom{pred_sym = "reachable", args = [b, c]},
   #dl_atom{pred_sym = "reachable", args = [c, d]},
   #dl_atom{pred_sym = "reachable", args = [a, c]},
   #dl_atom{pred_sym = "reachable", args = [b, d]},
   #dl_atom{pred_sym = "reachable", args = [a, d]},
   #dl_atom{pred_sym = "link", args = [a, b]},
   #dl_atom{pred_sym = "link", args = [b, c]},
   #dl_atom{pred_sym = "link", args = [c, d]}].

tc_prog() ->
  R1 = #dl_rule{head = reachable_atom("X", "Y"), body = [link_atom("X", "Y")]},
  R2 =
    #dl_rule{head = reachable_atom("X", "Y"),
             body = [link_atom("X", "Z"), reachable_atom("Z", "Y")]},
  [R1, R2].

reachable_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = reachable, args = [Arg1, Arg2]}.

link_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = link, args = [Arg1, Arg2]}.
