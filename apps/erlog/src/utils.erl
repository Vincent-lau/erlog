-module(utils).
-compile(export_all).

-include("data_repr.hrl").

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


to_string(#dl_atom{pred_sym = Sym, args = Args}) ->
  io_lib:format("~w(~s)", [Sym, to_string(Args)]);
to_string(#dl_rule{head = Head, body = Body}) ->
  Atoms = lists:map(fun to_string/1, Body),
  to_string(Head) ++ " :- " ++ string:join(Atoms, ", ") ++ ".";
to_string(L = [H|_]) when is_list(H) ->
  string:join(L, ",");
to_string(L = [H|_]) when is_atom(H) ->
  string:join(atom_to_list(L), ", ").
pretty_print(X) ->
  S = to_string(X),
  io:format("~s", [S]).
