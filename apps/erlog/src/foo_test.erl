-module(foo_test).

-compile(export_all).

-include("../include/data_repr.hrl").

read_and_lex(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = dl_lexer:string(Line),
      Tokens ++ read_and_lex(S)
  end.

read_input(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = erl_scan:string(Line),
      [lists:map(fun({_, _, Args}) -> Args end, Tokens) | read_input(S)]
  end.

start() ->
  ets:new(table, [named_table]),
  % open file and read program
  {ok, Stream} = file:open("apps/erlog/test/test_tc.dl", [read]),
  Tokens = read_and_lex(Stream),
  {ok, Prog} = dl_parser:parse(Tokens),
  file:close(Stream),
  % open file and read input
  {ok, Stream2} = file:open("apps/erlog/test/edge.facts", [read]),
  Input = read_input(Stream2),
  file:close(Stream2),
  % preprocess rules
  Prog2 = preproc:process(Prog),
  utils:ppt(Prog2),
  % create EDB from input relations
  EDB =
    db_ops:from_list(
      lists:map(fun(Args) -> #dl_atom{pred_sym = link, args = Args} end, Input)),
  Res = naive:eval_all(Prog2, EDB),
  db_ops:print_db(
    db_ops:get_rel_by_pred(reachable, Res)),
  ets:delete(table).

start2() ->
  A = #dl_atom{pred_sym = reachable, args = ["X", "Y"]},
  B = #dl_atom{pred_sym = link, args = ["X", "Y"]},
  R = #dl_rule{head = A, body = [B]},
  compile:file(utils),
  code:purge(utils),
  code:load_file(utils),
  utils:ppt(R),
  io:format("~n").
