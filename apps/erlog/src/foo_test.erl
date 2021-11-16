-module(foo_test).

-compile(export_all).

-include("../include/data_repr.hrl").
-include("../include/utils.hrl").

read_and_lex(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = dl_lexer:string(Line),
      Tokens ++ read_and_lex(S)
  end.

read_data(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = erl_scan:string(Line),
      [lists:map(fun({_, _, Args}) -> Args end, Tokens) | read_data(S)]
  end.

lex_and_parse(S) ->
  Tokens = read_and_lex(S),
  {ok, Prog} = dl_parser:parse(Tokens),
  Prog.

cons_db_from_data(Data, AtomName) ->
  db_ops:from_list(
    lists:map(fun(Args) -> dl_repr:cons_atom(AtomName, Args) end, Data)).

start() ->
  ets:new(dl_atom_names, [named_table]),
  % open file and read program
  {ok, Stream} = file:open("apps/erlog/test/eval_SUITE_data/rsg.dl", [read]),
  Prog = lex_and_parse(Stream),
  file:close(Stream),
  % preprocess rules
  Prog2 = preproc:process(Prog),
  io:format("Input program is:~n~s~n", [utils:to_string(Prog2)]),

  % open file and read input
  {ok, Stream2} = file:open("apps/erlog/test/eval_SUITE_data/rsg.facts", [read]),
  Facts = lex_and_parse(Stream2),
  file:close(Stream2),
  io:format("input data is ~p~n", [Facts]),
  % create EDB from input relations
  EDB = db_ops:from_list(Facts),
  Res = eval:eval_all(Prog2, EDB),
  ResQ = db_ops:get_rel_by_pred(reachable, Res),
  {ok, Stream3} = file:open("apps/erlog/test/eval_SUITE_data/rsg.csv", [read]),
  Output = read_data(Stream3),
  Ans = cons_db_from_data(Output, "rsg"),
  io:format("The result database is:~n~s~n", [db_ops:db_to_string(ResQ)]),
  ets:delete(dl_atom_names).

start2() ->
  A = #dl_atom{pred_sym = reachable, args = ["X", "Y"]},
  B = #dl_atom{pred_sym = link, args = ["X", "Y"]},
  R = #dl_rule{head = A, body = [B]},
  compile:file(utils),
  code:purge(utils),
  code:load_file(utils),
  utils:ppt(R),
  io:format("~n").

start3() ->
  ets:new(dl_atom_names, [named_table]),
  % open file and read program
  {ok, Stream} = file:open("apps/erlog/test/eval_SUITE_data/tc.dl", [read]),
  Tokens = read_and_lex(Stream),
  {ok, Prog} = dl_parser:parse(Tokens),
  file:close(Stream),
  io:format("~s~n", [utils:to_string(Prog)]),
  ets:delete(dl_atom_names).

start4() ->
  {ok, Stream} = file:open("apps/erlog/test/eval_SUITE_data/tc.facts", [read]),
  Tokens = read_and_lex(Stream),
  {ok, Facts} = dl_parser:parse(Tokens),
  file:close(Stream),
  io:format("facts are ~n~s~n", [utils:to_string(Facts)]).
