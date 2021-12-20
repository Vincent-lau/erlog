-module(utils).

-compile(export_all).

-include("../include/data_repr.hrl").

-include_lib("kernel/include/logger.hrl").

-spec to_string(dl_atom() | dl_rule() | dl_program() | [dl_atom()]) -> string().
to_string(Atoms = [#dl_atom{} | _]) ->
  AtomS = lists:map(fun to_string/1, Atoms),
  lists:join("\n", AtomS);
to_string(P = [#dl_rule{} | _]) ->
  Rules = lists:map(fun to_string/1, P),
  lists:join("\n", Rules);
to_string(A = #dl_atom{pred_sym = Sym}) ->
  io_lib:format("~w(~s)", [Sym, args_to_string(dl_repr:get_atom_args(A))]);
to_string(#dl_rule{head = Head, body = Body}) ->
  Atoms = lists:map(fun to_string/1, Body),
  to_string(Head) ++ " :- " ++ lists:join(", ", Atoms) ++ ".".

-spec args_to_string([string()]) -> string().
args_to_string(Args) ->
  lists:join(", ", lists:map(fun(A) -> "\"" ++ A ++ "\"" end, Args)).

% TODO remove this function
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
  io:format(standard_error, Format, Data).

dbg_log(Format, Data) ->
  % logger:set_primary_config(level, all),
  ?LOG_DEBUG(Format, Data).
