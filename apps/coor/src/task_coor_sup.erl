-module(task_coor_sup).

-export([start_link/0, init/1]).

-behaviour(supervisor).

start_link() ->
  supervisor:start_link(?MODULE, []).

init([]) ->
  MaxRestart = 5,
  MaxTime = 3600,
  SupFlags =
    #{strategy => simple_one_for_one,
      intensity => MaxRestart,
      period => MaxTime},

  ChildSpecs =
    [#{id => task_coor,
       start => {task_coor, start_link, []},
       restart => temporary,
       shutdown => 5000,
       type => worker}],

  {ok, {SupFlags, ChildSpecs}}.
