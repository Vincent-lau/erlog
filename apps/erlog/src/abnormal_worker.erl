-module(abnormal_worker).

-export([crasher/0, straggler/0, num_failures/1, num_stragglers/1]).

% time before crash
-define(MAX_HEALTHY_TIME, 10000).

% sleep time when receiving a wait task

% being slow, so sleep for 11 seconds
-define(MAX_STRAGGLE_TIME, 11000).
-define(STRAGGLE_PROB, 0.5).

-define(FAIL_PERCENT, 0.5).
-define(STRAGGLE_PERCENT, 0.5).

num_failures(N) ->
  trunc(N * ?FAIL_PERCENT).

num_stragglers(N) ->
  trunc(N * ?STRAGGLE_PERCENT).

crasher() ->
  T = rand:uniform(trunc(?MAX_HEALTHY_TIME / 2)) + trunc(?MAX_HEALTHY_TIME / 2),
  io:format("sleeping time before fail ~p~n", [T]),
  timer:sleep(T),
  io:format(standard_error, "damn, I am dying~n", []),
  erlang:halt().

straggler() ->
  T = rand:uniform(trunc(?MAX_STRAGGLE_TIME / 2)) + trunc(?MAX_STRAGGLE_TIME / 2),
  case rand:uniform() < ?STRAGGLE_PROB of
    true ->
      io:format(standard_error, "hey I am a straggler, I will sleep ~p ms first~n", [T]),
      timer:sleep(T),
      io:format("straggler waking up~n");
    false ->
      ok
  end.
