-module(eval_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

-import(dl_repr, [cons_atom/2, cons_rule/2, cons_args_from_list/1, get_rule_headname/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

get_proj_cols_test_() ->
  [{"test getting correct projection cols", {setup, fun start/0, fun multi_proj_cols/1}},
  {"test swapping column order", {setup, fun start/0, fun swap_cols/1}}].

get_overlap_cols_test_() ->
  [{"test getting correct overlapping cols",
    {setup, fun start/0, fun multi_overlap_cols/1}}].


static_relation_test_() ->
  {"test finding static relations", {setup, fun start/0, fun static_rel_not_from_input/1}}.
    
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

static_rel_not_from_input(_) ->
  Input = 
  "
    link2(X, Y) :- link(X, Y). 
    reachable(X, Y) :- reachable(X, Z), link(Z, Y).
    link(\"a\", \"b\").
    link(\"b\", \"c\").
  ",
  {Rules, F} = preproc:lex_and_parse(Input),
  Facts = db_ops:from_list(F),
  OneIterDB = eval:imm_conseq(Rules, Facts),
  StaticDB = eval:find_static_db(Rules, Facts, OneIterDB),

  {_, AnsF} = preproc:lex_and_parse("
    link(\"a\", \"b\").
    link(\"b\", \"c\").
    link2(\"a\", \"b\").
    link2(\"b\", \"c\").
  "),
  AnsFacts = db_ops:from_list(AnsF),
  ?_assertEqual(AnsFacts, StaticDB).
   


eval_to_end({Program, EDB}) ->
  FinalIDB = eval:eval_all(Program, EDB),
  ?_assertEqual(final_db(), FinalIDB).

trans_closure_rule({[_, Rule2], EDB}) ->
  DeltaAtoms = eval:eval_one_rule(Rule2, EDB),
  [?_assertEqual(db_ops:from_list([#dl_atom{pred_sym = reachable, args = [a, c]},
                                   #dl_atom{pred_sym = reachable, args = [b, d]}]),
                 DeltaAtoms)].

singleton_rule({[R1, _], EDB}) ->
  DeltaAtoms = eval:eval_one_rule(R1, EDB),
  [?_assertEqual(db_ops:from_list([#dl_atom{pred_sym = reachable, args = [a, b]},
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
  R = cons_rule(cons_atom("R", cons_args_from_list(["X", "Y", "L", "G", "T", "S", "P"])),
                [cons_atom("R1", cons_args_from_list(["X", "Y", "L"])),
                 cons_atom("R2", cons_args_from_list(["G", "T", "Y", "S", "L", "P"]))]),
  DB =
    db_ops:from_list([cons_atom("R1", cons_args_from_list(["a", "b", "c"])),
                      cons_atom("R2", cons_args_from_list(["d", "f", "b", "e", "c", "g"])),
                      cons_atom("R2", cons_args_from_list(["d", "f", "b", "e", "b", "g"]))]),
  NewDB = eval:eval_one_rule(R, DB),
  Ans =
    db_ops:from_list([cons_atom(get_rule_headname(R),
                                cons_args_from_list(["a", "b", "c", "d", "f", "e", "g"]))]),

  [?_assertEqual(Ans, NewDB)].

eval_no_overlapping_cols(_) ->
  R = cons_rule(cons_atom("R", cons_args_from_list(["M", "N", "P", "Q"])),
                [cons_atom("R1", cons_args_from_list(["M", "N"])),
                 cons_atom("R2", cons_args_from_list(["P", "Q"]))]),
  DB =
    db_ops:from_list([cons_atom("R1", cons_args_from_list(["a", "b"])),
                      cons_atom("R1", cons_args_from_list(["c", "d"])),
                      cons_atom("R2", cons_args_from_list(["e", "f"]))]),
  NewDB = eval:eval_one_rule(R, DB),
  Ans =
    db_ops:from_list([cons_atom(get_rule_headname(R),
                                cons_args_from_list(["a", "b", "e", "f"])),
                      cons_atom(get_rule_headname(R), cons_args_from_list(["c", "d", "e", "f"]))]),
  ?_assertEqual(Ans, NewDB).

% TODO use parser to generate these stuff
eval_complete_overlapping_cols(_) ->
  R = cons_rule(cons_atom("P", cons_args_from_list(["X", "Y"])),
                [cons_atom("Q", cons_args_from_list(["X", "Y"])),
                 cons_atom("R", cons_args_from_list(["X", "Y"]))]),
  DB =
    db_ops:from_list([cons_atom("Q", cons_args_from_list(["a", "b"])),
                      cons_atom("Q", cons_args_from_list(["b", "c"])),
                      cons_atom("R", cons_args_from_list(["b", "c"]))]),
  NewDB = eval:eval_one_rule(R, DB),
  Ans = db_ops:from_list([cons_atom("P", cons_args_from_list(["b", "c"]))]),
  ?_assertEqual(Ans, NewDB).

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
  Args1 = cons_args_from_list(["T", "Y", "L"]),
  Args2 = cons_args_from_list(["G", "T", "Y", "S", "L", "P"]),
  {Args1, Args2}.
