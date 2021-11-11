-module(utils).

-compile(export_all).

-include("../include/data_repr.hrl").

-include_lib("kernel/include/logger.hrl").

-spec to_string(dl_atom() | dl_rule()) -> string().
to_string(#dl_atom{pred_sym = Sym, args = Args}) ->
  io_lib:format("~w(~s)", [Sym, to_string(Args)]);
to_string(#dl_rule{head = Head, body = Body}) ->
  Atoms = lists:map(fun to_string/1, Body),
  to_string(Head) ++ " :- " ++ string:join(Atoms, ", ") ++ ".";
to_string(L = [H | _]) when is_list(H) ->
  string:join(L, ",");
to_string(L = [H | _]) when is_atom(H) ->
  L2 = lists:map(fun atom_to_list/1, L),
  string:join(L2, ", ").

-spec ppt(dl_atom() | dl_rule() | dl_program()) -> ok.
ppt(X = #dl_atom{}) ->
  S = to_string(X),
  io:format("~s~n", [S]);
ppt(X = #dl_rule{}) ->
  S = to_string(X),
  io:format("~s~n", [S]);
ppt(L) when is_list(L) ->
  lists:foreach(fun ppt/1, L).

dbg_ppt(X) ->
  S = to_string(X),
  dbg_format("~s~n", [S]).

dbg_format(Format, Data) ->
  logger:set_primary_config(level, all),
  logger:debug(Format, Data).

