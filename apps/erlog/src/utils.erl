-module(utils).
-compile(export_all).

-include("../include/data_repr.hrl").

filteri(Predi, L) ->
  filteri_rec(Predi, lists:zip(L, lists:seq(1, length(L)))).

filteri_rec(Predi, [{E, Idx} | T]) ->
  case Predi(Idx) of
    true ->
      [E | filteri_rec(Predi, T)];
    false ->
      filteri_rec(Predi, T)
    end;
filteri_rec(_, []) -> [].


-spec to_string(dl_atom() | dl_rule()) -> string().
to_string(#dl_atom{pred_sym = Sym, args = Args}) ->
  io_lib:format("~w(~s)", [Sym, to_string(Args)]);
to_string(#dl_rule{head = Head, body = Body}) ->
  Atoms = lists:map(fun to_string/1, Body),
  to_string(Head) ++ " :- " ++ string:join(Atoms, ", ") ++ ".";
to_string(L = [H|_]) when is_list(H) ->
  string:join(L, ",");
to_string(L = [H|_]) when is_atom(H) ->
  L2 = lists:map(fun atom_to_list/1, L),
  string:join(L2, ", ").

-spec ppt(dl_atom() | dl_rule()) -> ok.
ppt(X) ->
  S = to_string(X),
  io:format("~s~n", [S]).

-define(debug, 1).
-ifdef(debug).
dbg_ppt(X) ->
  S = to_string(X),
  io:format(standard_error, "~s~n", [S]).
dbg_format(Format, Data) ->
  io:format(standard_error, Format, Data).
-else.
dbg_ppt(_) ->
  ok.
dbg_format(_, _) -> 
  ok.
-endif.
