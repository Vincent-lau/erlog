-module(dl_parser_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").

% %% test description
parse_test_() ->
  [{"parse one rule", {setup, fun start/0, fun parse_one_rule/1}},
   {"parse program", {setup, fun start_multi/0, fun parse_multiple_rules/1}}].

start() ->
  [{dl_const, 1, reachable},
   {'(', 1},
   {dl_var, 1, "X"},
   {',', 1},
   {dl_var, 1, "Y"},
   {')', 1},
   {':-', 1},
   {dl_const, 1, link},
   {'(', 1},
   {dl_var, 1, "X"},
   {',', 1},
   {dl_var, 1, "Y"},
   {')', 1},
   {'.', 1}].

start_multi() ->
  tc1_tks("X", "Y", 1) ++ tc2_tks("X", "Y", "Z", 1).

parse_one_rule(R) ->
  {ok, A} = dl_parser:parse(R),
  [?_assertEqual([#dl_rule{head = #dl_atom{pred_sym = reachable, args = ["X", "Y"]},
                           body = [#dl_atom{pred_sym = link, args = ["X", "Y"]}]}],
                 A)].

parse_multiple_rules(R) ->
  {ok, P1} = dl_parser:parse(R),
  R1 = #dl_rule{head = reachable_atom("X", "Y"), body = [link_atom("X", "Y")]},
  R2 =
    #dl_rule{head = reachable_atom("X", "Y"),
             body = [link_atom("X", "Z"), reachable_atom("Z", "Y")]},
  utils:dbg_ppt(lists:nth(2, P1)),
  P2 = [R1, R2],
  [?_assertEqual(lists:nth(1, P1), R1),
   ?_assertEqual(lists:nth(2, P1), R2),
   ?_assertEqual(P1, P2)].

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

reachable_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = reachable, args = [Arg1, Arg2]}.

link_atom(Arg1, Arg2) ->
  #dl_atom{pred_sym = link, args = [Arg1, Arg2]}.

tc1_tks(Sym1, Sym2, LineNo) ->
  [{dl_const, LineNo, reachable},
   {'(', LineNo},
   {dl_var, LineNo, Sym1},
   {',', LineNo},
   {dl_var, LineNo, Sym2},
   {')', LineNo},
   {':-', LineNo},
   {dl_const, LineNo, link},
   {'(', LineNo},
   {dl_var, LineNo, Sym1},
   {',', LineNo},
   {dl_var, LineNo, Sym2},
   {')', LineNo},
   {'.', LineNo}].

tc2_tks(Sym1, Sym2, Sym3, LineNo) ->
  [{dl_const, LineNo, reachable},
   {'(', LineNo},
   {dl_var, LineNo, Sym1},
   {',', LineNo},
   {dl_var, LineNo, Sym2},
   {')', LineNo},
   {':-', LineNo},
   {dl_const, LineNo, link},
   {'(', LineNo},
   {dl_var, LineNo, Sym1},
   {',', LineNo},
   {dl_var, LineNo, Sym3},
   {')', LineNo},
   {',', LineNo},
   {dl_const, LineNo, reachable},
   {'(', LineNo},
   {dl_var, LineNo, Sym3},
   {',', LineNo},
   {dl_var, LineNo, Sym2},
   {')', LineNo},
   {'.', LineNo}].
