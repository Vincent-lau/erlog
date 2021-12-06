-module(listsi).

-compile(export_all).

%%----------------------------------------------------------------------
%% Function: index_of
%% Purpose: Find the index of an element in a list
%% Args:
%% Returns:
%% This is O(N) algorithm! Use with care.
%%----------------------------------------------------------------------
-spec index_of(any(), list()) -> integer() | not_found.
index_of(N, L) ->
  index_of(N, L, 1).

index_of(_, [], _) ->
  not_found;
index_of(N, [H | _], I) when N =:= H ->
  I;
index_of(N, [_ | T], I) ->
  index_of(N, T, I + 1).

-spec filteri(fun((any(), integer()) -> boolean()), list()) -> list().
filteri(Predi, L) ->
  foldri(fun(E, Idx, Acc) ->
            case Predi(E, Idx) of
              true -> [E | Acc];
              false -> Acc
            end
         end,
         [],
         L).

filteri_rec(Predi, [{E, Idx} | T]) ->
  case Predi(E, Idx) of
    true ->
      [E | filteri_rec(Predi, T)];
    false ->
      filteri_rec(Predi, T)
  end;
filteri_rec(_, []) ->
  [].

-spec mapi(fun((any(), integer()) -> any()), [{any(), integer}]) -> [any()].
mapi(Fi, L) ->
  foldri(fun(Ele, Idx, Acc) -> [Fi(Ele, Idx) | Acc] end, [], L).

mapi_rec(Fi, [{E, Idx} | T]) ->
  [Fi(E, Idx) | mapi_rec(Fi, T)];
mapi_rec(_, []) ->
  [].

-spec foldli(fun((any(), integer(), any()) -> any()), any(), [any()]) -> any().
foldli(Fi, Acc, L) ->
  foldli_rec(Fi, Acc, lists:zip(L, lists:seq(1, length(L)))).

foldri(Fi, Acc, L) ->
  foldri_rec(Fi, Acc, lists:zip(L, lists:seq(1, length(L)))).

foldli_rec(_, Acc, []) ->
  Acc;
foldli_rec(Fi, Acc, [{E, Idx} | T]) ->
  NewAcc = Fi(E, Idx, Acc),
  foldli_rec(Fi, NewAcc, T).

foldri_rec(_, Acc, []) ->
  Acc;
foldri_rec(Fi, Acc, [{E, Idx} | T]) ->
  NewAcc = foldri_rec(Fi, Acc, T),
  Fi(E, Idx, NewAcc).

filtermapi(Fi, L) ->
  foldri(fun(E, Idx, Acc) ->
            case Fi(E, Idx) of
              false -> Acc;
              true -> [E | Acc];
              {true, Val} -> [Val | Acc]
            end
         end,
         [],
         L).
