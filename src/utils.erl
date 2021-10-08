-module(utils).
-compile(export_all).

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