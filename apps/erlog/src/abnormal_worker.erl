-module(abnormal_worker).

-export([crasher/0, straggler/0, num_failures/1, num_stragglers/1, num_failures/2,
         num_stragglers/2]).

% time before crash
-define(MAX_HEALTHY_TIME, 30000).

% sleep time when receiving a wait task

% being slow, so sleep for 11 seconds
-define(MAX_STRAGGLE_TIME, 11000).
-define(STRAGGLE_PROB, 0.2).

-spec num_failures(integer(), float()) -> integer().
num_failures(N, Percent) ->
  trunc(N * Percent).

num_failures(N) ->
  num_failures(N, 0.5).

-spec num_stragglers(integer(), float()) -> integer().
num_stragglers(N, Percent) ->
  trunc(N * Percent).

num_stragglers(N) ->
  num_stragglers(N, 0.5).

crasher() ->
  T = rand:uniform(?MAX_HEALTHY_TIME) + 1000,
  lager:debug("sleeping time before fail ~p~n", [T]),
  timer:sleep(T),
  lager:debug("damn, I am dying ~p~n", [node()]),
  erlang:halt().

straggler() ->
  T = rand:uniform(trunc(?MAX_STRAGGLE_TIME / 2)) + trunc(?MAX_STRAGGLE_TIME / 2),
  case rand:uniform() < ?STRAGGLE_PROB of
    true ->
      lager:debug("hey I am a straggler, I will sleep ~p ms first~n", [T]),
      timer:sleep(T),
      lager:debug("straggler waking up~n");
    false ->
      ok
  end.
