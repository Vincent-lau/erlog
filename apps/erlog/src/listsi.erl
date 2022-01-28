-module(listsi).

-export([index_of/2, filteri/2, mapi/2, foldli/3, foldri/3, filtermapi/2]).

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

-spec mapi(fun((any(), integer()) -> any()), list()) -> [any()].
mapi(Fi, L) ->
  foldri(fun(Ele, Idx, Acc) -> [Fi(Ele, Idx) | Acc] end, [], L).


-spec foldli(fun((any(), integer(), any()) -> any()), any(), list()) -> any().
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
