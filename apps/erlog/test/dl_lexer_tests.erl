-module(dl_lexer_tests).

-include_lib("eunit/include/eunit.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

string_test_() ->
  [{"testing lexing one rule", {setup, fun start/0, fun lex_one_rule/1}},
   {"lexing one rule with multiple body atoms",
    {setup, fun start/0, fun lex_one_rule_multi_body/1}},
   {"lexing from file a TC program",
    {setup, fun start_file/0, fun cleanup_file/1, fun lex_tc_from_file/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start() ->
  ok.

start_file() ->
  {ok, Stream} = file:open("apps/erlog/test/eval_SUITE_data/tc.dl", [read]),
  Stream.

cleanup_file(S) ->
  file:close(S).

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%

lex_one_rule(_) ->
  R = "reachable(X, Y) :- link(X, Y).",
  {ok, Tokens, _} = dl_lexer:string(R),
  ?_assertEqual(tc1_tks("X", "Y", 1), Tokens).

lex_one_rule_multi_body(_) ->
  R = "reachable(X, Z) :- link(X, Y), reachable(Y, Z).",
  {ok, Tokens, _} = dl_lexer:string(R),
  [?_assertEqual(tc2_tks("X", "Z", "Y", 1), Tokens)].

lex_tc_from_file(S) ->
  Tokens = read_and_lex(S),
  ?_assertEqual(tc_facts_tks() ++ tc1_tks("X", "Y", 1) ++ tc2_tks("X", "Y", "Z", 1),
                Tokens).

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

read_and_lex(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = dl_lexer:string(Line),
      Tokens ++ read_and_lex(S)
  end.

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

tc_facts_tks() ->
  [{dl_const, 1, link}, {'(', 1}, {dl_const, 1, a}, {',', 1}, {dl_const, 1, b}, {')', 1},
   {'.', 1}, {dl_const, 1, link}, {'(', 1}, {dl_const, 1, b}, {',', 1}, {dl_const, 1, c},
   {')', 1}, {'.', 1}, {dl_const, 1, link}, {'(', 1}, {dl_const, 1, c}, {',', 1},
   {dl_const, 1, c}, {')', 1}, {'.', 1}, {dl_const, 1, link}, {'(', 1}, {dl_const, 1, c},
   {',', 1}, {dl_const, 1, d}, {')', 1}, {'.', 1}, {dl_const, 1, link}, {'(', 1},
   {dl_const, 1, d}, {',', 1}, {dl_const, 1, e}, {')', 1}, {'.', 1}, {dl_const, 1, link},
   {'(', 1}, {dl_const, 1, e}, {',', 1}, {dl_const, 1, f}, {')', 1}, {'.', 1}].
