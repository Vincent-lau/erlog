-module(foo_test).

-compile(export_all).

-include("../include/data_repr.hrl").

% TODO write proper test for parser and lexer
start() ->
  {ok, _} = leex:file('dl_lexer.xrl'),
  {ok, _} = compile:file(dl_lexer),
  {ok, _} = yecc:file('dl_parser.yrl'),
  {ok, _} = compile:file(dl_parser),
  code:purge(dl_lexer), code:purge(dl_parser),
  code:load_file(dl_lexer),
  code:load_file(dl_parser),
  {ok, L, _} = dl_lexer:string("reachable(X, Y) :- link(X,Y)."),
  io:format("token is ~p~n", [L]),
  dl_parser:parse(L).

start2() ->
  A = #dl_atom{pred_sym = reachable, args = ["X", "Y"]},
  B = #dl_atom{pred_sym = link, args = ["X", "Y"]},
  R = #dl_rule{head = A, body = [B]},
  compile:file(utils),
  code:purge(utils),
  code:load_file(utils),
  utils:ppt(R),
  io:format("~n").