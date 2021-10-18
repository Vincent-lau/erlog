-module(utils_tests).

-include_lib("eunit/include/eunit.hrl").

filteri_test_() ->
  [test_even()].

test_even() ->
  L = utils:filteri(fun(X) -> X rem 2 == 0 end, lists:seq(1, 10)),
  ?_assertEqual([2, 4, 6, 8, 10], L).
