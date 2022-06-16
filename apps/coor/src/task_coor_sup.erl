-module(task_coor_sup).

-export([start_link/1, init/1]).

-behaviour(supervisor).

start_link(MFA = {_, _, _}) ->
  supervisor:start_link(?MODULE, MFA).

init({M, F, A}) ->
  MaxRestart = 5,
  MaxTime = 3600,
  SupFlags =
    #{strategy => simple_one_for_one,
      intensity => MaxRestart,
      period => MaxTime},

  ChildSpecs =
    [#{id => task_coor,
       start => {M, F, A},
       restart => temporary,
       shutdown => 5000,
       type => worker,
       modules => [M]}],

  {ok, {SupFlags, ChildSpecs}}.
