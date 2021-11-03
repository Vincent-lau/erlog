-module(listsi_tests).

-include_lib("eunit/include/eunit.hrl").


foldri_test_() ->
  [get_idx_sum()].

filteri_test_() ->
  [test_even()].



mapi_test_() ->
  [double_odd_idx_ele()].

filtermapi_test_() ->
  [remove_odd_idx_and_double_rest()].

test_even() ->
  L = listsi:filteri(fun(_, I) -> I rem 2 == 0 end, lists:seq(1, 10)),
  ?_assertEqual([2, 4, 6, 8, 10], L).

get_idx_sum() ->
  L = lists:seq(1, 10),
  R = listsi:foldri(fun(_, I, Acc) -> I + Acc end, 0, L),
  ?_assertEqual(lists:sum(L), R).

double_odd_idx_ele() ->
  L = lists:seq(1, 10),
  R = listsi:mapi(fun(E, I) ->
                          case I rem 2 of
                            0 -> E * 2;
                            1 -> E
                          end
                       end,
                       L),
  [?_assertEqual([1, 4, 3, 8, 5, 12, 7, 16, 9, 20], R)].

remove_odd_idx_and_double_rest() ->
  L = lists:seq(1, 10),
  R = listsi:filtermapi(fun(E, I) ->
                                case I rem 2 of
                                  0 -> {true, E * 2};
                                  1 -> false
                                end
                             end,
                             L),
  ?_assertEqual([4, 8, 12, 16, 20], R).
